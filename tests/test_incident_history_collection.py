"""수집된 Pod 이력의 끝에서 끝까지 검증: provider → 인증된 agent ingress → PostgreSQL → history.

손으로 만든 complete 입력이 아니라 실제 provider의 query/normalize/build_response 출력을 쓴다.
격리된 Postgres와 Redis(합성 데이터)가 없으면 건너뛴다.
"""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from test_incident_history_db import (  # noqa: F401 - fixtures shared with the DB tests
    db,
    real_case,
    seed_real_gateway,
)
from test_incident_history_provider import Failure, collect, module, pod, pod_list  # noqa: F401

REDIS_URL = os.environ.get("INCIDENT_HISTORY_TEST_REDIS_URL", "")
pytestmark = pytest.mark.skipif(
    not (os.environ.get("INCIDENT_HISTORY_TEST_DATABASE_URL") and REDIS_URL),
    reason="INCIDENT_HISTORY_TEST_DATABASE_URL과 INCIDENT_HISTORY_TEST_REDIS_URL 미설정",
)
PASSWORD = "Synthetic-Passw0rd!"


class Harness:
    """One isolated case reached only through the public gateway routes."""

    def __init__(self, client: TestClient, database: Any, provider_module: Any) -> None:
        self.client = client
        self.db = database
        self.module = provider_module
        self.suffix = uuid.uuid4().hex[:8]
        self.case = seed_real_gateway(database, real_case(self.suffix))
        self.correlation = f"collect-{self.suffix}"
        self.now = datetime.now(UTC)
        self.step = 0
        database.save_rca_report(
            self.correlation,
            self.case.workspace,
            "synthetic",
            "none",
            {
                "incident": {
                    "incident_id": f"inc-{self.suffix}",
                    "cluster_id": self.case.cluster,
                    "first_seen_at": (self.now - timedelta(minutes=2)).isoformat(),
                    "symptom": "synthetic",
                    "severity": "warning",
                }
            },
        )

    def collect(
        self,
        minutes_ago: int,
        pods_by_namespace: dict[str, Any],
        *,
        selector: str | None = None,
        transform: Callable[[dict], None] | None = None,
    ) -> int:
        """Collect through the real provider, then send it as the agent would."""
        kubernetes = collect(self.module, pods_by_namespace, selector=selector)
        at = (self.now - timedelta(minutes=minutes_ago)).isoformat()
        kubernetes.setdefault("cluster", {})["collected_at"] = at
        if transform is not None:
            transform(kubernetes)
        self.step += 1
        body = {
            "correlation_id": self.correlation,
            "evidence_key": f"cut-{self.step}-{self.suffix}",
            "source_id": "agent",
            "window_start": at,
            "kubernetes": kubernetes,
        }
        response = self.client.post(
            "/agent/evidence", json=body, headers={"x-agent-token": self.case.agent_token}
        )
        return response.status_code

    def history(self) -> dict[str, Any]:
        self.client.cookies.clear()
        login = self.client.post(
            "/auth/login", json={"email": self.case.viewer[1], "password": PASSWORD}
        )
        assert login.status_code == 200
        response = self.client.get(f"/incidents/{self.correlation}/history")
        assert response.status_code == 200
        return response.json()

    def lifecycle(self) -> dict[str, tuple[str, str | None]]:
        result: dict[str, tuple[str, str | None]] = {}
        for item in self.history()["resources"]:
            subject = item["subject"]
            key = subject["resource"]["uid"] if subject["kind"] == "resource" else subject["name"]
            result[key] = (item["lifecycle"], item["deleted_at_bound"])
        return result

    def events(self) -> list[tuple[str, str]]:
        return [
            (event["event_type"], event["subject"].get("resource", {}).get("uid") or event["title"])
            for event in self.history()["events"]
        ]


@pytest.fixture
def harness(db: Any, module: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[Harness]:  # noqa: F811
    from packages.events.in_memory import InMemoryEventBus

    sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "src/services/gateway/api-gateway")]
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    gateway = __import__("gateway")
    with TestClient(gateway.create_app(event_bus=InMemoryEventBus()), base_url="https://t") as app:
        yield Harness(app, db, module)


def test_a_proven_namespace_records_replacement_and_deletion(harness: Harness) -> None:
    ns = "default"
    assert (
        harness.collect(
            30, {ns: pod_list(pod("api-0", uid="u-old"), pod("worker-0", uid="u-worker"))}
        )
        == 200
    )
    assert harness.collect(20, {ns: pod_list(pod("api-0", uid="u-new"))}) == 200
    assert harness.collect(10, {ns: pod_list()}) == 200

    body = harness.history()

    assert body["completeness"]["status"] == "partial"  # a scoped cut is never complete
    assert sorted(harness.events()) == sorted(
        [
            ("add", "u-old"),
            ("add", "u-worker"),
            ("update", "u-new"),
            ("delete", "u-worker"),
            ("delete", "u-new"),
        ]
    )
    assert harness.lifecycle() == {
        "u-old": ("deleted", "gap_bounded"),  # replaced: the deletion itself was never seen
        "u-worker": ("deleted", "observed"),
        "u-new": ("deleted", "observed"),
    }
    first = [e for e in body["events"] if e["event_type"] == "add"]
    assert {e["metadata"]["observation_basis"] for e in first} == {"first_observed"}
    assert all("created" not in e["title"] for e in first)


NEGATIVES: dict[str, dict[str, Any]] = {
    "label_selector": {
        "namespaces": lambda: {"default": pod_list(pod("a", uid="ua"))},
        "selector": "app=a",
    },
    "continuation": {
        "namespaces": lambda: {
            "default": pod_list(pod("a", uid="ua"), metadata={"continue": "next"})
        }
    },
    "other_namespace": {"namespaces": lambda: {"other": pod_list()}},
    "malformed_list": {"namespaces": lambda: {"default": []}},
    "failed_query": {"namespaces": lambda: {"default": Failure(500)}},
    "rbac_denied": {"namespaces": lambda: {"default": Failure(403)}},
}


@pytest.mark.parametrize("name", sorted(NEGATIVES))
def test_an_unproven_cut_never_invents_a_deletion(harness: Harness, name: str) -> None:
    spec = NEGATIVES[name]
    harness.collect(
        20,
        {"default": pod_list(pod("a", uid="ua"), pod("b", uid="ub"))},
    )

    status = harness.collect(10, spec["namespaces"](), selector=spec.get("selector"))

    assert status == 200
    assert not [event for event in harness.events() if event[0] == "delete"]
    assert harness.lifecycle() == {"ua": ("present", None), "ub": ("present", None)}


def test_a_producer_without_proof_never_invents_a_deletion(harness: Harness) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"))})

    def drop_proof(kubernetes: dict) -> None:
        kubernetes.pop("collection_coverage", None)

    harness.collect(10, {"default": pod_list()}, transform=drop_proof)

    assert not [event for event in harness.events() if event[0] == "delete"]
    assert harness.lifecycle() == {"ua": ("present", None)}


def test_a_limited_pod_list_never_invents_a_deletion(harness: Harness) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"))})
    flood = [pod(f"f-{i}", namespace="busy", uid=f"uf-{i}") for i in range(501)]

    status = harness.collect(10, {"default": pod_list(), "busy": pod_list(*flood)})

    assert status == 200
    body = harness.history()
    assert not [event for event in body["events"] if event["event_type"] == "delete"]
    # The flood exceeds the history limit; that is reported, never read as completeness.
    assert body["truncated"] is True and "truncated" in body["completeness"]["reasons"]
    still_live = harness.db.connection()
    with still_live as conn:
        from sqlalchemy import text

        deleted = conn.execute(
            text(
                "select count(*) from cluster_inventory_resources "
                "where workspace_id = :w and name = 'a' and deleted_at is not null"
            ),
            {"w": harness.case.workspace},
        ).scalar_one()
    assert deleted == 0


def test_a_proof_kept_by_a_limited_payload_is_dropped_at_the_ingress(harness: Harness) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"))})

    def limited(kubernetes: dict) -> None:
        kubernetes["collection_limits"] = {
            "truncated": True,
            "lists": {"pods": {"truncated": True}},
        }

    harness.collect(10, {"default": pod_list()}, transform=limited)

    assert not [event for event in harness.events() if event[0] == "delete"]


def test_uid_less_pods_stay_locators_and_are_never_promoted(harness: Harness) -> None:
    harness.collect(20, {"default": pod_list(pod("ghost"))})

    body = harness.history()

    subjects = [event["subject"] for event in body["events"]]
    assert [subject["kind"] for subject in subjects] == ["inventory_locator"]
    assert all(event["resource"] is None for event in body["events"])
    assert [item["subject"]["kind"] for item in body["resources"]] == ["inventory_locator"]
    assert body["resources"][0]["subject"]["name"] == "ghost"


def _corrupt(path: str, value: Any) -> Callable[[dict], None]:
    def apply(kubernetes: dict) -> None:
        entry = kubernetes["collection_coverage"][0]
        if path == "__whole__":
            kubernetes["collection_coverage"] = value
        else:
            entry[path] = value

    return apply


CORRUPT_PROOFS: dict[str, Callable[[dict], None]] = {
    "reasons_int": _corrupt("reason_codes", 7),
    "reasons_true": _corrupt("reason_codes", True),
    "reasons_non_string": _corrupt("reason_codes", [7]),
    "reasons_string": _corrupt("reason_codes", "collection_rbac_denied"),
    "observed_string": _corrupt("observed", "yes"),
    "complete_string": _corrupt("complete", "true"),
    "delete_safe_int": _corrupt("delete_safe", 1),
    "truncated_string": _corrupt("truncated", "false"),
    "selector_int": _corrupt("label_selector", 7),
    "whole_string": _corrupt("__whole__", "junk"),
    "whole_number": _corrupt("__whole__", 7),
    "whole_junk_list": _corrupt("__whole__", [None, 7, "x"]),
}


def _assert_observed_but_nothing_deleted(harness: Harness, status: int) -> None:
    assert status == 200  # malformed proof never becomes an unhandled 500
    events = harness.events()
    assert not [event for event in events if event[0] == "delete"]
    assert ("update", "ua2") in events  # the cut itself was still observed and stored
    assert harness.lifecycle()["ub"] == ("present", None)


@pytest.mark.parametrize("name", sorted(CORRUPT_PROOFS))
def test_a_corrupted_proof_is_stored_as_an_observation_and_deletes_nothing(
    harness: Harness, name: str
) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"), pod("b", uid="ub"))})

    status = harness.collect(
        10, {"default": pod_list(pod("a", uid="ua2"))}, transform=CORRUPT_PROOFS[name]
    )

    _assert_observed_but_nothing_deleted(harness, status)


MALFORMED_COLLECTOR_LISTS: dict[str, dict[str, Any]] = {
    "metadata_string": {"metadata": "invalid"},
    "remaining_string": {"metadata": {"remainingItemCount": "invalid"}},
    "observed_with_rbac_reason": {
        "kyro_collection_status": {"observed": True, "reason_codes": ["collection_rbac_denied"]}
    },
    "status_reasons_int": {"kyro_collection_status": {"observed": True, "reason_codes": 7}},
    "remaining_negative": {"metadata": {"remainingItemCount": -1}},
    "remaining_float": {"metadata": {"remainingItemCount": 2.0}},
}


@pytest.mark.parametrize("name", sorted(MALFORMED_COLLECTOR_LISTS))
def test_a_malformed_collector_list_deletes_nothing_through_the_ingress(
    harness: Harness, name: str
) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"), pod("b", uid="ub"))})

    status = harness.collect(
        10, {"default": pod_list(pod("a", uid="ua2"), **MALFORMED_COLLECTOR_LISTS[name])}
    )

    _assert_observed_but_nothing_deleted(harness, status)


@pytest.mark.parametrize(
    "junk",
    [
        "x",
        7,
        {"lists": 7},
        [],
        True,
        {"truncated": True, "lists": {}},
        {"truncated": "bad", "lists": {}},
        {"truncated": False, "lists": {"events": {}}},
    ],
)
def test_junk_collection_limits_at_the_ingress_never_return_500(
    harness: Harness, junk: Any
) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"), pod("b", uid="ub"))})

    def set_limits(kubernetes: dict) -> None:
        kubernetes["collection_limits"] = junk

    status = harness.collect(10, {"default": pod_list(pod("a", uid="ua2"))}, transform=set_limits)

    assert status == 200
    assert not [event for event in harness.events() if event[0] == "delete"]


def test_limits_that_name_only_other_lists_keep_a_valid_pod_proof(harness: Harness) -> None:
    harness.collect(20, {"default": pod_list(pod("a", uid="ua"))})

    def other_list_limited(kubernetes: dict) -> None:
        kubernetes["collection_limits"] = {
            "truncated": True,
            "lists": {"events": {"truncated": True, "original_count": 900, "returned_count": 200}},
        }

    harness.collect(10, {"default": pod_list()}, transform=other_list_limited)

    assert ("delete", "ua") in harness.events()
    assert harness.lifecycle()["ua"] == ("deleted", "observed")
