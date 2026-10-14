"""실제 게이트웨이 합성 서버에 사건 한 건을 공개 HTTP로 넣는 로더(테스트가 아니다).

실행: INCIDENT_HISTORY_TEST_DATABASE_URL=... uv run python -c \
  "import sys; sys.path[:0]=['src','tests']; import test_incident_history_sample as t; t.load_real_sample()"

Kubernetes payload는 실제 provider가 가짜 Kubernetes API를 대상으로 만든 출력이다. RCA report는
incident-worker가 만드는 것이라 이 게이트웨이 프로세스에 없으므로 DB에 직접 저장하고, 나머지는
공개 agent 경로와 실제 인증을 거친다.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import UTC, datetime, timedelta

from test_incident_history_db import DB_URL, _kubernetes_payload, real_case
from test_incident_history_provider import collect, pod, pod_list, provider_module

CORRELATION = "real-incident-1"


def load_real_sample(base_url: str = "http://127.0.0.1:18766") -> str:
    os.environ["DATABASE_URL"] = DB_URL
    from domains.registry import Database

    case = real_case()
    now = datetime.now(UTC)
    fact = {
        "uid": "evt-real-1",
        "api_version": "v1",
        "namespace": "default",
        "name": "api-0.crashloop",
        "type": "Warning",
        "count": 4,
        "last_occurrence_at": (now - timedelta(minutes=6)).isoformat(),
    }

    def post(key: str, minutes_ago: int, pods: list, *, with_events: bool) -> None:
        at = (now - timedelta(minutes=minutes_ago)).isoformat()
        with provider_module() as module:
            kubernetes = collect(
                module,
                {
                    "default": pod_list(*pods),
                    "legacy": pod_list(pod("ghost", namespace="legacy")),
                },
            )
        kubernetes["cluster"]["collected_at"] = at
        if with_events:
            # Fresh at the collection time, as a real capture taken in the same pass would be.
            kubernetes["event_capture"] = _kubernetes_payload(
                now - timedelta(minutes=minutes_ago), "x", events=[fact]
            )["event_capture"]
        body = {
            "correlation_id": CORRELATION,
            "evidence_key": key,
            "source_id": "synthetic-agent",
            "window_start": at,
            "kubernetes": kubernetes,
        }
        request = urllib.request.Request(
            f"{base_url}/agent/evidence",
            data=json.dumps(body).encode(),
            headers={"content-type": "application/json", "x-agent-token": case.agent_token},
        )
        urllib.request.urlopen(request).read()  # noqa: S310 - fixed loopback URL

    # api-0 is recreated with a new UID and worker-0 vanishes from a proven namespace list.
    # ghost, in its own namespace, has no UID at all: it must stay a locator.
    post(
        "sample-1",
        14,
        [pod("api-0", uid="uid-api-old"), pod("worker-0", uid="uid-worker")],
        with_events=False,
    )
    post("sample-2", 8, [pod("api-0", uid="uid-api-new")], with_events=True)
    Database().save_rca_report(
        CORRELATION,
        case.workspace,
        "synthetic_root_cause",
        "none",
        {
            "incident": {
                "incident_id": "inc-real-1",
                "cluster_id": case.cluster,
                "first_seen_at": (now - timedelta(minutes=5)).isoformat(),
                "resource_kind": "Pod",
                "resource_name": "api-0",
                "namespace": "default",
                "symptom": "CrashLoopBackOff",
                "severity": "warning",
            }
        },
    )
    return CORRELATION
