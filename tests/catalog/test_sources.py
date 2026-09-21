"""카탈로그 입력 어댑터.

카탈로그가 원천을 다시 조회하지 않고 이미 수집된 결과를 읽는지 확인한다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from domains.datacatalog.sources import (  # noqa: E402
    ASSET_BY_SOURCE,
    CollectedSource,
    FixtureSource,
    source_tables_present,
)


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._rows


class FakeConn:
    """실행된 SQL 과 바인딩을 기록한다."""

    def __init__(self, rows=None, scalar=None):
        self.rows = rows or []
        self._scalar = scalar
        self.executed: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params or {}))
        if self._scalar is not None:
            return FakeResult(self._scalar)
        return FakeResult(self.rows)


def test_fixture_source_reads_file(tmp_path: Path) -> None:
    payload = [{"asset_id": "a", "row_key": "k", "cluster_id": "local", "payload": {}}]
    (tmp_path / "loki.json").write_text(json.dumps(payload), encoding="utf-8")
    assert FixtureSource(tmp_path).fetch("loki", "2026-08-09") == payload


def test_fixture_source_missing_file_is_empty(tmp_path: Path) -> None:
    assert FixtureSource(tmp_path).fetch("tempo", "2026-08-09") == []


def test_collected_source_reads_inventory_for_kubernetes() -> None:
    class Ts:
        def isoformat(self):
            return "2026-08-09T04:00:00+00:00"

    conn = FakeConn(rows=[{
        "cluster_id": "local", "name": "svc-0", "namespace": "prod",
        "uid": "uid-1", "summary": {"name": "svc-0"}, "raw": {}, "observed_at": Ts(),
    }])
    rows = CollectedSource(conn).fetch("kubernetes", "2026-08-09")

    assert rows[0]["asset_id"] == ASSET_BY_SOURCE["kubernetes"]
    assert rows[0]["row_key"] == "uid-1"
    sql = conn.executed[0][0]
    assert "cluster_inventory_resources" in sql
    # 원천이 아니라 수집 결과를 읽는다.
    assert "evidence_windows" not in sql


def test_collected_source_falls_back_to_namespaced_name_when_uid_missing() -> None:
    class Ts:
        def isoformat(self):
            return "2026-08-09T04:00:00+00:00"

    conn = FakeConn(rows=[{
        "cluster_id": "local", "name": "svc-0", "namespace": "prod",
        "uid": None, "summary": {}, "raw": {"a": 1}, "observed_at": Ts(),
    }])
    rows = CollectedSource(conn).fetch("kubernetes", "2026-08-09")
    assert rows[0]["row_key"] == "prod/svc-0"
    # summary 가 비면 raw 로 떨어진다.
    assert rows[0]["payload"] == {"a": 1}


@pytest.mark.parametrize("source_id", ["prometheus", "loki", "tempo"])
def test_collected_source_reads_evidence_windows(source_id: str) -> None:
    conn = FakeConn(rows=[{
        "cluster_id": "local", "evidence_key": f"{source_id}-0",
        "window_start": "2026-08-09T04:00:00+00:00", "payload": {"x": 1},
    }])
    rows = CollectedSource(conn).fetch(source_id, "2026-08-09")

    assert rows[0]["asset_id"] == ASSET_BY_SOURCE[source_id]
    assert rows[0]["row_key"] == f"{source_id}-0"
    sql, params = conn.executed[0]
    assert "evidence_windows" in sql
    assert params["s"] == source_id
    # 하루치만 읽는다.
    assert params["prefix"] == "2026-08-09%"


def test_collected_source_ignores_unknown_source() -> None:
    conn = FakeConn()
    assert CollectedSource(conn).fetch("datadog", "2026-08-09") == []
    assert conn.executed == []


def test_source_tables_present_requires_both_tables() -> None:
    assert source_tables_present(FakeConn(scalar=2)) is True
    assert source_tables_present(FakeConn(scalar=1)) is False
    assert source_tables_present(FakeConn(scalar=0)) is False
