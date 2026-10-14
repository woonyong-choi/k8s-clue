"""Pod namespace 범위 증명: provider가 만든 증거만 삭제 근거가 될 수 있다.

실제 provider의 query → normalize → merge → build_response를 가짜 Kubernetes API로
구동한다. 손으로 만든 complete fixture가 아니다.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
COLLECTED_AT = "2026-10-15T00:00:00+00:00"


class Failure(int):
    """An HTTP status the fake Kubernetes API answers with instead of a body."""


PodResponse = Any


AGENT_DIR = ROOT / "src/services/target/cluster-agent"


@contextlib.contextmanager
def provider_module() -> Iterator[Any]:
    """Import the agent's service-local modules, then remove them again."""
    import conftest

    saved = {name: sys.modules.pop(name, None) for name in conftest.SERVICE_LOCAL_MODULES}
    sys.path.insert(0, str(AGENT_DIR))
    try:
        loaded = importlib.import_module("providers.kubernetes_providers")
        loaded.kubernetes_api_base_url = lambda: "https://k8s.test"
        loaded.service_account_token = lambda: "synthetic-token"
        yield loaded
    finally:
        sys.path.remove(str(AGENT_DIR))
        for name in conftest.SERVICE_LOCAL_MODULES:
            sys.modules.pop(name, None)
            if saved[name] is not None:
                sys.modules[name] = saved[name]


@pytest.fixture
def module() -> Iterator[Any]:
    with provider_module() as loaded:
        yield loaded


def pod(name: str, namespace: str = "default", uid: str | None = None, **extra: Any) -> dict:
    meta: dict[str, Any] = {"name": name, "namespace": namespace, "resourceVersion": "1"}
    if uid is not None:
        meta["uid"] = uid
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {**meta, **extra},
        "spec": {"nodeName": "node-a"},
        "status": {"phase": "Running"},
    }


def pod_list(*pods: dict, **top: Any) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": list(pods), **top}


def collect(
    module: Any,
    pods_by_namespace: dict[str, PodResponse],
    *,
    selector: str | None = None,
) -> dict[str, Any]:
    """Run each namespace query through the real provider; a failing query is dropped as in the collector."""

    def handler(request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        if "namespaces" in parts and parts[-1] == "pods" and "metrics.k8s.io" not in parts:
            namespace = parts[parts.index("namespaces") + 1]
            response = pods_by_namespace[namespace]
            if isinstance(response, Failure):
                return httpx.Response(int(response), json={"message": "synthetic failure"})
            return httpx.Response(200, content=json.dumps(response))
        if "metrics.k8s.io" in request.url.path:
            return httpx.Response(404)
        return httpx.Response(200, json={"items": []})

    provider = module.KubernetesSnapshotProvider(
        cluster_id="cluster-1", transport=httpx.MockTransport(handler)
    )
    results = provider.empty_results()

    async def run() -> None:
        async with httpx.AsyncClient() as client:
            for namespace in pods_by_namespace:
                query = module.KubernetesSnapshotQuery(
                    query_name=f"ns-{namespace}",
                    description="synthetic",
                    namespace=namespace,
                    label_selector=selector,
                )
                try:
                    payload = await provider.query(client, query)
                except httpx.HTTPStatusError:
                    continue
                payload["collected_at"] = COLLECTED_AT
                provider.append_result(results, query, payload)

    asyncio.run(run())
    return provider.build_response(results)


def entries(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["namespace"]: entry for entry in snapshot.get("collection_coverage", [])}


def assert_not_delete_proof(entry: dict[str, Any]) -> None:
    assert entry["complete"] is False
    assert entry["delete_safe"] is False


def test_unfiltered_complete_list_proves_the_namespace(module: Any) -> None:
    snapshot = collect(module, {"default": pod_list(pod("api-0", uid="u1"))})

    entry = entries(snapshot)["default"]
    assert entry["complete"] is True and entry["delete_safe"] is True
    assert entry["observed"] is True and entry["truncated"] is False
    assert entry["reason_codes"] == [] and entry["label_selector"] is None
    assert [item["uid"] for item in snapshot["pods"]] == ["u1"]


def test_an_empty_complete_list_proves_absence(module: Any) -> None:
    entry = entries(collect(module, {"default": pod_list()}))["default"]

    assert entry["complete"] is True and entry["delete_safe"] is True


def test_only_pods_are_declared_and_workloads_are_never_complete(module: Any) -> None:
    snapshot = collect(module, {"default": pod_list(pod("api-0", uid="u1"))})

    assert {entry["collection"] for entry in snapshot["collection_coverage"]} == {"pods"}


def test_a_continuation_token_is_truncated_and_never_proof(module: Any) -> None:
    listing = pod_list(pod("api-0", uid="u1"), metadata={"continue": "next-page"})

    entry = entries(collect(module, {"default": listing}))["default"]

    assert_not_delete_proof(entry)
    assert entry["truncated"] is True and "continuation" in entry["reason_codes"]


def test_remaining_item_count_is_truncated(module: Any) -> None:
    listing = pod_list(pod("api-0", uid="u1"), metadata={"remainingItemCount": 3})

    entry = entries(collect(module, {"default": listing}))["default"]

    assert_not_delete_proof(entry)
    assert entry["truncated"] is True


def test_a_label_selector_never_proves_the_namespace(module: Any) -> None:
    entry = entries(collect(module, {"default": pod_list()}, selector="app=api"))["default"]

    assert_not_delete_proof(entry)
    assert entry["label_selector"] == "app=api"
    assert "label_selector" in entry["reason_codes"]


@pytest.mark.parametrize("malformed", [[], "text", 7, None])
def test_a_non_object_response_is_never_an_empty_proof(module: Any, malformed: Any) -> None:
    entry = entries(collect(module, {"default": malformed}))["default"]

    assert_not_delete_proof(entry)
    assert entry["observed"] is False and "malformed_list" in entry["reason_codes"]


@pytest.mark.parametrize(
    "listing",
    [
        {"kind": "PodList", "metadata": {}},
        {"kind": "PodList", "items": "not-a-list"},
        {"kind": "NodeList", "items": []},
        {"kind": "PodList", "items": ["junk"]},
    ],
)
def test_a_malformed_list_shape_is_not_proof(module: Any, listing: dict) -> None:
    entry = entries(collect(module, {"default": listing}))["default"]

    assert_not_delete_proof(entry)
    assert entry["observed"] is False


@pytest.mark.parametrize("status", [403, 404, 500, 503])
def test_a_failed_query_yields_no_coverage_at_all(module: Any, status: int) -> None:
    snapshot = collect(module, {"default": Failure(status)})

    assert "collection_coverage" not in snapshot
    assert snapshot["pods"] == []


def test_a_row_from_another_namespace_breaks_the_proof(module: Any) -> None:
    listing = pod_list(pod("api-0", uid="u1"), pod("intruder", namespace="other", uid="u9"))

    entry = entries(collect(module, {"default": listing}))["default"]

    assert_not_delete_proof(entry)
    assert "namespace_mismatch" in entry["reason_codes"]


def test_a_row_without_identity_breaks_the_proof(module: Any) -> None:
    entry = entries(collect(module, {"default": pod_list(pod("no-uid"))}))["default"]

    assert_not_delete_proof(entry)
    assert "identity_missing" in entry["reason_codes"]


def test_each_namespace_is_judged_alone_after_merge(module: Any) -> None:
    snapshot = collect(
        module,
        {
            "alpha": pod_list(pod("a", namespace="alpha", uid="ua")),
            "beta": pod_list(pod("b", namespace="beta", uid="ub"), metadata={"continue": "x"}),
            "gamma": Failure(500),
        },
    )

    found = entries(snapshot)
    assert set(found) == {"alpha", "beta"}
    assert found["alpha"]["complete"] is True
    assert_not_delete_proof(found["beta"])


def test_a_pod_list_limit_invalidates_every_namespace(module: Any) -> None:
    many = [pod(f"p-{index}", uid=f"u-{index}") for index in range(module.MAX_KUBERNETES_PODS + 1)]

    snapshot = collect(
        module,
        {"default": pod_list(*many), "calm": pod_list(pod("c", namespace="calm", uid="uc"))},
    )

    assert "pods" in snapshot["collection_limits"]["lists"]
    for entry in snapshot["collection_coverage"]:
        assert_not_delete_proof(entry)
        assert entry["truncated"] is True and "payload_limit" in entry["reason_codes"]


def test_the_payload_size_limit_invalidates_the_proof(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = module.limit_payload_size
    monkeypatch.setattr(
        module,
        "limit_payload_size",
        lambda payload, **kw: real(payload, **{**kw, "max_bytes": 800}),
    )
    pods = [pod(f"p-{index}", uid=f"u-{index}") for index in range(20)]

    snapshot = collect(module, {"default": pod_list(*pods)})

    assert "pods" in snapshot["collection_limits"]["lists"]
    assert_not_delete_proof(entries(snapshot)["default"])


def test_an_unlimited_snapshot_keeps_its_proof_after_build_response(module: Any) -> None:
    snapshot = collect(module, {"default": pod_list(pod("api-0", uid="u1"))})

    assert "collection_limits" not in snapshot
    assert entries(snapshot)["default"]["complete"] is True


def test_an_unavailable_query_is_not_observed(module: Any) -> None:
    provider = module.KubernetesSnapshotProvider(cluster_id="cluster-1")
    query = module.KubernetesSnapshotQuery(query_name="ns", description="d", namespace="default")

    snapshot = provider.normalize_payload(
        {"status": "unavailable", "reason": "kubernetes api is not configured"}, query
    )

    entry = snapshot["collection_coverage"][0]
    assert_not_delete_proof(entry)
    assert entry["observed"] is False


def test_get_json_marks_a_non_object_body(module: Any) -> None:
    provider = module.KubernetesSnapshotProvider(
        cluster_id="c", transport=httpx.MockTransport(lambda _r: httpx.Response(200, json=[]))
    )

    async def fetch() -> dict:
        async with httpx.AsyncClient(transport=provider.transport) as client:
            return await provider.get_json(client, "https://k8s.test", {}, "/api/v1/x")

    result = asyncio.run(fetch())

    assert result["items"] == [] and result["kyro_list_malformed"] is True


MALFORMED_LISTS = {
    "metadata_string": {"metadata": "invalid"},
    "metadata_list": {"metadata": []},
    "remaining_string": {"metadata": {"remainingItemCount": "invalid"}},
    "remaining_bool": {"metadata": {"remainingItemCount": True}},
    "remaining_negative": {"metadata": {"remainingItemCount": -1}},
    "remaining_float": {"metadata": {"remainingItemCount": 2.0}},
    "continue_int": {"metadata": {"continue": 7}},
    "status_not_object": {"kyro_collection_status": "bad"},
    "status_observed_string": {"kyro_collection_status": {"observed": "yes", "reason_codes": []}},
    "status_reasons_int": {"kyro_collection_status": {"observed": True, "reason_codes": 7}},
    "status_reasons_non_string": {
        "kyro_collection_status": {"observed": True, "reason_codes": [7]}
    },
    "observed_with_rbac_reason": {
        "kyro_collection_status": {"observed": True, "reason_codes": ["collection_rbac_denied"]}
    },
    "unobserved_without_reason": {"kyro_collection_status": {"observed": False}},
}


@pytest.mark.parametrize("name", sorted(MALFORMED_LISTS))
def test_a_malformed_list_field_is_never_proof(module: Any, name: str) -> None:
    listing = pod_list(pod("api-0", uid="u1"), **MALFORMED_LISTS[name])

    snapshot = collect(module, {"default": listing})

    entry = entries(snapshot)["default"]
    assert_not_delete_proof(entry)
    assert entry["reason_codes"]
    assert [item["uid"] for item in snapshot["pods"]] == ["u1"]  # the observation is kept


@pytest.mark.parametrize(
    "row",
    [
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default", "uid": 7},
        },
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": 7, "namespace": "default", "uid": "u"},
        },
        {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": 7, "uid": "u"}},
        {"apiVersion": "v1", "kind": "Pod", "metadata": "invalid"},
        {"apiVersion": "v1", "kind": "Pod"},
    ],
)
def test_a_row_with_wrongly_typed_identity_is_never_proof(module: Any, row: dict) -> None:
    snapshot = collect(module, {"default": pod_list(row)})

    assert_not_delete_proof(entries(snapshot)["default"])


def test_a_zero_remaining_count_is_still_a_complete_list(module: Any) -> None:
    listing = pod_list(pod("api-0", uid="u1"), metadata={"remainingItemCount": 0, "continue": ""})

    entry = entries(collect(module, {"default": listing}))["default"]

    assert entry["complete"] is True and entry["reason_codes"] == []
