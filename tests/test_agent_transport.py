from __future__ import annotations

import asyncio
import gzip
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from conftest import load_file
from fastapi import FastAPI
from fastapi.testclient import TestClient

from domains.identity.dependencies import ClusterAgentIdentity, require_cluster_agent
from domains.rca.router import router
from domains.rca.transport import MAX_REQUEST_BYTES
from packages.runtime.dependencies import get_db, get_events


@pytest.fixture(scope="module")
def agent_module() -> Any:
    return load_file(
        Path(__file__).resolve().parents[1] / "src/services/target/cluster-agent/agent.py",
        "transport_agent",
    )


class EvidenceDatabase:
    def __init__(self) -> None:
        self.windows: dict[str, Any] = {}

    def get_evidence_window(self, key: str) -> Any:
        return self.windows.get(key)

    def record_evidence_event_once(self, **values: Any) -> Any:
        envelope = values["event_envelope"]
        return self.windows.setdefault(
            values["evidence_key"],
            {
                "event_id": envelope.event_id,
                "correlation_id": envelope.correlation_id,
                "payload": values["payload"],
            },
        )


@pytest.fixture
def gateway() -> tuple[FastAPI, EvidenceDatabase]:
    app = FastAPI()
    app.include_router(router)
    db = EvidenceDatabase()
    app.dependency_overrides[require_cluster_agent] = lambda: ClusterAgentIdentity("w", "c")
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_events] = lambda: SimpleNamespace(source="gateway")
    return app, db


# #9: 실제 HTTP 송신과 수신 계약 및 재전송 키 보존.
def test_agent_compressed_submission_preserves_payload_and_deduplicates(
    agent_module: Any, gateway: tuple[FastAPI, EvidenceDatabase]
) -> None:
    app, db = gateway
    payload = {
        "evidence_key": "observation-1",
        "workspace_id": "untrusted",
        "cluster_id": "untrusted",
        "kubernetes": {"events": [{"message": "Back-off pulling image" * 100}]},
        "collection_status": {"complete": False},
    }

    async def run() -> None:
        async with agent_module.HttpManagementPlaneClient("http://gateway") as client:
            await client.client.aclose()
            client.client = httpx.AsyncClient(transport=httpx.ASGITransport(app))
            await client.send_evidence(payload)
            await client.send_evidence(payload)

    asyncio.run(run())
    assert len(db.windows) == 1
    saved = db.windows["w:c:observation-1"]["payload"]
    assert saved["kubernetes"] == payload["kubernetes"]
    assert saved["collection_status"] == {"complete": False}
    assert saved["workspace_id"] == "w"


@pytest.mark.parametrize(
    ("body", "encoding", "status"),
    [
        (b"{}", "identity", 200),
        (gzip.compress(b"{}"), "gzip", 200),
        (b"invalid", "gzip", 400),
        (gzip.compress(b"{}")[:-2], "gzip", 400),
        (gzip.compress(b"{}") + b"trailing", "gzip", 400),
        (gzip.compress(b" " * (MAX_REQUEST_BYTES + 1)), "gzip", 413),
        (b" " * (MAX_REQUEST_BYTES + 1), "identity", 413),
        (b"{}", "br", 415),
    ],
)
def test_evidence_http_encoding_is_bounded(
    gateway: tuple[FastAPI, EvidenceDatabase], body: bytes, encoding: str, status: int
) -> None:
    app, _ = gateway
    response = TestClient(app).post(
        "/agent/evidence",
        content=body,
        headers={"content-type": "application/json", "content-encoding": encoding},
    )
    assert response.status_code == status


def test_agent_retry_does_not_recollect_or_change_observation_key(
    agent_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MANAGEMENT_BASE_URL", "http://gateway")
    agent = agent_module.TargetClusterAgent()
    assert agent.collector.providers["kubernetes"].queries
    collected = 0
    sent: list[dict[str, Any]] = []

    async def collect() -> dict[str, Any]:
        nonlocal collected
        collected += 1
        return {"kubernetes": {"pods": [{"uid": "pod-1"}]}}

    async def send(payload: dict[str, Any]) -> None:
        sent.append(dict(payload))
        if len(sent) == 1:
            raise httpx.ReadTimeout("response lost")

    agent.collector.collect = collect
    client = SimpleNamespace(send_evidence=send)

    async def run() -> None:
        with pytest.raises(httpx.ReadTimeout):
            await agent.collect_once(client)
        await agent.collect_once(client)
        await agent.collect_once(client)

    asyncio.run(run())
    assert collected == 2
    assert sent[0] == sent[1]
    assert sent[2]["evidence_key"] != sent[1]["evidence_key"]


def test_agent_unconfigured_kubernetes_does_not_report_complete(
    agent_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MANAGEMENT_BASE_URL", "http://gateway")
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    agent = agent_module.TargetClusterAgent()

    payload = asyncio.run(agent.collector.collect())

    assert payload["collection_status"]["complete"] is False
    assert payload["collection_status"]["failed_providers"] == ["kubernetes"]
