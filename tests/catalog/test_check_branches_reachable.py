"""검사 질의의 모든 판정 분기에 실제로 도달하는 입력이 있는지.

이 파일이 생긴 이유가 있다. `duplicate_candidates` 는 한때 유일 제약과 같은 키로
묶어서 어떤 데이터에서도 0행이었고, 그건 우연히 발견했다. 그 뒤에도 같은 종류의
결함이 `source_coverage` 에 남아 있었다 — CHRONICALLY_TRUNCATED 가 DEGRADED 보다
뒤에 있어서 도달할 수 없었다.

두 번 다 "질의를 읽어 보니 괜찮아 보인다" 로는 못 잡았다. 그래서 분기마다 실제
데이터를 만들어 그 판정이 나오는지 확인한다. 나오지 않으면 죽은 분기다.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from domains.datacatalog.checks import load_sql

NOW = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)
DATE = NOW.date()


def _dag(conn, dag_run_id: str, *, status: str, finished: bool = True) -> None:
    conn.execute(
        text(
            "INSERT INTO catalog_dag_runs (dag_run_id, dag_id, logical_date, status, "
            "started_at, finished_at) VALUES (:d,'catalog',:ld,:s,:t,:f)"
        ),
        {"d": dag_run_id, "ld": DATE, "s": status, "t": NOW, "f": NOW if finished else None},
    )


def _source(conn, source_id: str = "loki") -> None:
    conn.execute(
        text(
            "INSERT INTO catalog_data_sources (source_id, name, source_type, owner, "
            "collection_interval_seconds, enabled, created_at) "
            "VALUES (:s,:s,'log','platform',300,true,:t) ON CONFLICT DO NOTHING"
        ),
        {"s": source_id, "t": NOW},
    )


def _collection(conn, run_id: str, dag_run_id: str, *, status: str) -> None:
    _source(conn)
    conn.execute(
        text(
            "INSERT INTO catalog_collection_runs (run_id, dag_run_id, source_id, "
            "logical_date, status, attempt, finished_at, created_at) "
            "VALUES (:r,:d,'loki',:ld,:s,1,:t,:t)"
        ),
        {"r": run_id, "d": dag_run_id, "ld": DATE, "s": status, "t": NOW},
    )


def _quality(conn, dag_run_id: str, *, status: str = "passed", severity: str = "warning") -> None:
    conn.execute(
        text(
            "INSERT INTO catalog_quality_results (result_id, dag_run_id, check_name, "
            "check_type, asset_id, subject_key, status, severity, first_seen_dag_run_id, "
            "checked_at) VALUES (:r,:d,'x','SOURCE_COVERAGE','-','-',:s,:sev,:d,:t)"
        ),
        {"r": f"qr-{dag_run_id}-{status}-{severity}", "d": dag_run_id,
         "s": status, "sev": severity, "t": NOW},
    )


def _snapshot(conn, run_id: str) -> None:
    conn.execute(
        text(
            "INSERT INTO catalog_raw_snapshots (snapshot_id, run_id, s3_uri, "
            "content_hash, byte_size, created_at) "
            "VALUES (:s,:r,'file:///x',:h,1,:t)"
        ),
        {"s": f"snap-{run_id}", "r": run_id, "h": f"h-{run_id}", "t": NOW},
    )


def _findings(conn) -> dict[str, str]:
    rows = conn.execute(
        text(load_sql("run_consistency")), {"logical_date": DATE}
    ).mappings().all()
    return {r["dag_run_id"]: r["finding"] for r in rows}


def test_소스는_실패했는데_실행이_성공(conn):
    _dag(conn, "d1", status="SUCCESS")
    _collection(conn, "r1", "d1", status="FAILED")
    _snapshot(conn, "r1")
    _quality(conn, "d1")
    assert _findings(conn)["d1"] == "SOURCE_FAILED_BUT_RUN_SUCCESS"


def test_성공인데_검사가_하나도_없음(conn):
    _dag(conn, "d2", status="SUCCESS")
    _collection(conn, "r2", "d2", status="SUCCESS")
    _snapshot(conn, "r2")
    assert _findings(conn)["d2"] == "SUCCESS_WITHOUT_ANY_CHECK"


def test_종료_시각이_없음(conn):
    _dag(conn, "d3", status="PARTIAL", finished=False)
    _collection(conn, "r3", "d3", status="SUCCESS")
    _quality(conn, "d3")
    assert _findings(conn)["d3"] == "TERMINAL_WITHOUT_FINISH"


def test_성공인데_원본_스냅샷이_없음(conn):
    _dag(conn, "d4", status="SUCCESS")
    _collection(conn, "r4", "d4", status="SUCCESS")
    _quality(conn, "d4")
    assert _findings(conn)["d4"] == "SUCCESS_WITHOUT_SNAPSHOT"


def test_이번_실행에서_처음_난_error(conn):
    _dag(conn, "d5", status="SUCCESS")
    _collection(conn, "r5", "d5", status="SUCCESS")
    _snapshot(conn, "r5")
    _quality(conn, "d5", status="failed", severity="error")
    assert _findings(conn)["d5"] == "SUCCESS_WITH_NEW_ERRORS"


def test_정상_실행은_잡히지_않는다(conn):
    """양성만 확인하면 항상 참을 반환하는 질의도 통과한다."""
    _dag(conn, "d6", status="SUCCESS")
    _collection(conn, "r6", "d6", status="SUCCESS")
    _snapshot(conn, "r6")
    _quality(conn, "d6", status="passed", severity="warning")
    assert "d6" not in _findings(conn)


@pytest.mark.parametrize(
    "branch",
    ["SOURCE_FAILED_BUT_RUN_SUCCESS", "SUCCESS_WITHOUT_ANY_CHECK",
     "TERMINAL_WITHOUT_FINISH", "SUCCESS_WITHOUT_SNAPSHOT", "SUCCESS_WITH_NEW_ERRORS"],
)
def test_질의에_선언된_분기가_전부_위_테스트에_있다(branch):
    """분기를 추가하고 도달 테스트를 안 만들면 여기서 걸린다."""
    import pathlib

    sql = load_sql("run_consistency")
    assert branch in sql
    here = pathlib.Path(__file__).read_text("utf-8")
    assert branch in here, f"{branch} 에 도달하는 테스트가 없다"


# 원천 커버리지 -------------------------------------------------------------
#
# 상시 잘림 판정이 한때 다른 분기 뒤에 있어 도달할 수 없었다. 그때는 CASE 를
# 테스트에 복사해 두고 그 복사본을 검사했는데, 그러면 질의 파일이 바뀌어도
# 테스트는 통과한다. 그래서 파일을 그대로 실행한다.


# 한 원천은 하루에 한 번 실행된다. dag_run_id 가 `dag_id__logical_date` 이고
# catalog_collection_runs 에 (dag_run_id, source_id) 유일 제약이 있기 때문이다.
# 그래서 커버리지 창(7일) 안에 들어올 수 있는 실행은 최대 8회다. 처음 이 검사를
# 쓸 때는 한 dag_run 아래에 실행 10건을 넣었는데, 스키마가 애초에 허용하지 않는
# 모양이었다. 제약이 없는 낡은 DB 에서만 통과하던 검사였다.
COVERAGE_WINDOW_DAYS = 7


def _coverage(conn, source_id: str, *, enabled: bool, statuses: list[str]) -> None:
    assert len(statuses) <= COVERAGE_WINDOW_DAYS + 1, (
        f"창 안에 넣을 수 있는 실행은 최대 {COVERAGE_WINDOW_DAYS + 1}회다"
    )
    conn.execute(
        text(
            "INSERT INTO catalog_data_sources (source_id, name, source_type, owner, "
            "collection_interval_seconds, enabled, created_at) "
            "VALUES (:s,:s,'log','platform',300,:e,:t)"
        ),
        {"s": source_id, "e": enabled, "t": NOW},
    )
    for offset, status in enumerate(statuses):
        day = DATE - timedelta(days=offset)
        dag_run_id = f"catalog__{day}"
        conn.execute(
            text(
                "INSERT INTO catalog_dag_runs (dag_run_id, dag_id, logical_date, status, "
                "started_at) VALUES (:d,'catalog',:ld,'SUCCESS',:t) ON CONFLICT DO NOTHING"
            ),
            {"d": dag_run_id, "ld": day, "t": NOW},
        )
        conn.execute(
            text(
                "INSERT INTO catalog_collection_runs (run_id, dag_run_id, source_id, "
                "logical_date, status, attempt, finished_at, created_at) "
                "VALUES (:r,:d,:s,:ld,:st,1,:t,:t)"
            ),
            {"r": f"{source_id}-{offset}", "d": dag_run_id, "s": source_id,
             "ld": day, "st": status, "t": NOW},
        )


def _coverage_findings(conn) -> dict[str, str]:
    rows = conn.execute(
        text(load_sql("source_coverage")), {"logical_date": DATE}
    ).mappings().all()
    return {r["source_id"]: r["finding"] for r in rows}


@pytest.mark.parametrize(
    ("source_id", "enabled", "statuses", "expected"),
    [
        ("s-silent", True, [], "ENABLED_BUT_SILENT"),
        ("s-disabled", False, ["SUCCESS"] * 3, "DISABLED_BUT_RUNNING"),
        ("s-never", True, ["FAILED"] * 3, "NEVER_HEALTHY"),
        # 잘림 5 / 성공 2 -> 잘림 71%, 성공률 29%. 두 분기가 다 맞는 입력이라야
        # 순서를 검사할 수 있다. 상시 잘림이 먼저 잡혀야 한다.
        ("s-trunc", True, ["TRUNCATED"] * 5 + ["SUCCESS"] * 2, "CHRONICALLY_TRUNCATED"),
        # 실패 3 / 성공 4 -> 성공률 57%. 잘림이 없으므로 성능 저하로 잡힌다.
        ("s-degraded", True, ["FAILED"] * 3 + ["SUCCESS"] * 4, "DEGRADED"),
    ],
)
def test_원천_커버리지_판정이_모두_도달_가능하다(conn, source_id, enabled, statuses, expected):
    _coverage(conn, source_id, enabled=enabled, statuses=statuses)
    assert _coverage_findings(conn).get(source_id) == expected


def test_건강한_원천은_잡히지_않는다(conn):
    """정상 데이터에서 아무것도 안 나와야 한다. 양성만 보면 항상 참인 질의도 통과한다."""
    _coverage(conn, "s-ok", enabled=True, statuses=["SUCCESS"] * 7 + ["NO_DATA"])
    assert "s-ok" not in _coverage_findings(conn)
