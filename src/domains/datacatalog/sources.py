"""카탈로그가 무엇을 입력으로 읽는지 결정하는 어댑터.

카탈로그는 원천을 다시 조회하지 않는다. 이미 수집된 결과를 읽는다.
같은 원천을 두 경로가 각각 긁으면 세 가지가 깨진다.

1. 판정과 검사가 서로 다른 시점의 데이터를 본다. "검사가 통과했다"가
   "판정에 쓰인 데이터가 통과했다"를 뜻하지 않게 된다.
2. 과거 날짜를 다시 처리할 수 없다. 원천은 과거 상태를 갖고 있지 않으므로
   현재 값에 과거 날짜 도장을 찍게 된다.
3. 원천 부하가 두 배가 된다.

그래서 입력을 어댑터로 분리했다. 로컬 재현에는 fixture 를, 실제 배포에는
이미 적재된 수집 결과를 읽는 어댑터를 쓴다.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Connection

# 수집 결과가 들어 있는 원본 테이블. cluster-agent 가 채운다.
KUBERNETES_TABLE = "cluster_inventory_resources"
EVIDENCE_TABLE = "evidence_windows"

ASSET_BY_SOURCE = {
    "kubernetes": "kubernetes.resource_snapshot",
    "prometheus": "prometheus.metric_series",
    "loki": "loki.log_stream",
    "tempo": "tempo.trace",
}


class CatalogSource(Protocol):
    """logical_date 하루치 관측을 돌려준다."""

    mode: str

    def fetch(self, source_id: str, logical_date: str) -> list[dict[str, Any]]: ...


class FixtureSource:
    """로컬 재현용. 고정된 관측을 읽는다."""

    mode = "fixture"

    def __init__(self, root: Path) -> None:
        self.root = root

    def fetch(self, source_id: str, logical_date: str) -> list[dict[str, Any]]:
        path = self.root / f"{source_id}.json"
        if not path.exists():
            return []
        return json.loads(path.read_text(encoding="utf-8"))


class CollectedSource:
    """이미 수집된 결과를 읽는다. 원천에는 접근하지 않는다.

    kubernetes 는 inventory 스냅샷의 리소스 행에서, 나머지 세 소스는
    evidence_windows 의 payload 에서 가져온다. 둘 다 cluster-agent 가
    30초 주기로 채우는 테이블이다.
    """

    mode = "collected"

    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def fetch(self, source_id: str, logical_date: str) -> list[dict[str, Any]]:
        asset_id = ASSET_BY_SOURCE.get(source_id)
        if asset_id is None:
            return []
        if source_id == "kubernetes":
            return self._fetch_inventory(asset_id, logical_date)
        return self._fetch_evidence(source_id, asset_id, logical_date)

    def _fetch_inventory(self, asset_id: str, logical_date: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            text(
                f"""
                SELECT cluster_id, name, namespace, uid, summary, raw, observed_at
                FROM {KUBERNETES_TABLE}
                WHERE observed_at >= CAST(:d AS date)
                  AND observed_at <  CAST(:d AS date) + INTERVAL '1 day'
                  AND deleted_at IS NULL
                ORDER BY observed_at, name
                """
            ),
            {"d": logical_date},
        ).mappings().all()
        return [
            {
                "asset_id": asset_id,
                "row_key": row["uid"] or f"{row['namespace']}/{row['name']}",
                "cluster_id": row["cluster_id"],
                "payload": row["summary"] or row["raw"] or {},
                "observed_at": row["observed_at"].isoformat(),
            }
            for row in rows
        ]

    def _fetch_evidence(
        self, source_id: str, asset_id: str, logical_date: str
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            text(
                f"""
                SELECT cluster_id, evidence_key, window_start, payload
                FROM {EVIDENCE_TABLE}
                WHERE source_id = :s
                  AND window_start LIKE :prefix
                ORDER BY window_start, evidence_key
                """
            ),
            {"s": source_id, "prefix": f"{logical_date}%"},
        ).mappings().all()
        return [
            {
                "asset_id": asset_id,
                "row_key": row["evidence_key"],
                "cluster_id": row["cluster_id"],
                "payload": row["payload"] or {},
                "observed_at": row["window_start"],
            }
            for row in rows
        ]


def source_tables_present(conn: Connection) -> bool:
    """수집 결과 테이블이 이 데이터베이스에 있는지 확인한다.

    카탈로그를 별도 데이터베이스로 띄운 로컬 환경에서는 없다.
    그때는 fixture 로 떨어뜨리되, 떨어졌다는 사실을 호출한 쪽이 알아야 한다.
    """
    found = conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name IN (:a, :b)"
        ),
        {"a": KUBERNETES_TABLE, "b": EVIDENCE_TABLE},
    ).scalar()
    return found == 2
