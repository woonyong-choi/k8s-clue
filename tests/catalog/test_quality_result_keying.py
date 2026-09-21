"""검사 결과 적재 키.

한 검사가 한 자산에서 위반을 여러 건 찾는다. 03 은 필드마다, 02 는 누락
필드마다, 08 은 리소스마다다. 유일 제약이 자산 단위면 두 번째 위반부터
앞의 것을 덮어써서 자산당 1건만 남는다.

이 파일은 그 손실이 없는지 본다. "통과·실패를 모두 남긴다"는 설계가 실패
쪽에서 깨지지 않는지 확인하는 자리다.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from domains.datacatalog.checks import _subject_key, _write_result

NOW = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)


def _seed_dag_run(conn, dag_run_id: str = "dag-1") -> str:
    conn.execute(
        text(
            "INSERT INTO catalog_dag_runs (dag_run_id, dag_id, logical_date, status, started_at) "
            "VALUES (:d, 'catalog', :ld, 'SUCCESS', :t) ON CONFLICT DO NOTHING"
        ),
        {"d": dag_run_id, "ld": NOW.date(), "t": NOW},
    )
    return dag_run_id


def _write(conn, dag_run_id, *, subject_key, finding="TYPE_CHANGED", asset="ops.evidence"):
    _write_result(
        conn,
        dag_run_id=dag_run_id,
        check_name="03_schema_drift",
        check_type="SCHEMA_DRIFT",
        asset_id=asset,
        subject_key=subject_key,
        status="failed",
        severity="error",
        finding=finding,
        observed=None,
        expected=None,
        checked_at=NOW,
    )


def test_한_자산의_필드별_위반이_모두_남는다(conn):
    dag = _seed_dag_run(conn)
    for field in ("spec.replicas", "metadata.labels.app", "status.phase"):
        _write(conn, dag, subject_key=field)

    rows = conn.execute(
        text(
            "SELECT subject_key FROM catalog_quality_results "
            "WHERE dag_run_id = :d AND asset_id = 'ops.evidence' ORDER BY subject_key"
        ),
        {"d": dag},
    ).scalars().all()
    assert rows == ["metadata.labels.app", "spec.replicas", "status.phase"]


def test_같은_대상은_덮어쓴다(conn):
    """멱등성은 유지되어야 한다. 재실행이 행을 늘리면 안 된다."""
    dag = _seed_dag_run(conn)
    _write(conn, dag, subject_key="spec.replicas", finding="TYPE_CHANGED")
    _write(conn, dag, subject_key="spec.replicas", finding="FIELD_REMOVED")

    rows = conn.execute(
        text(
            "SELECT finding FROM catalog_quality_results "
            "WHERE dag_run_id = :d AND asset_id = 'ops.evidence'"
        ),
        {"d": dag},
    ).scalars().all()
    assert rows == ["FIELD_REMOVED"]


def test_자산_단위_검사는_대상키가_하나다(conn):
    dag = _seed_dag_run(conn)
    _write_result(
        conn, dag_run_id=dag, check_name="01_source_coverage", check_type="SOURCE_COVERAGE",
        asset_id="-", subject_key="-", status="passed", severity="warning",
        finding=None, observed=None, expected=None, checked_at=NOW,
    )
    count = conn.execute(
        text("SELECT count(*) FROM catalog_quality_results WHERE check_name = '01_source_coverage'")
    ).scalar_one()
    assert count == 1


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"field_path": "spec.replicas"}, "spec.replicas"),
        ({"resource_uid": "uid-7"}, "uid-7"),
        ({"schema_version": 3}, "3"),
        ({"asset_id": "ops.evidence"}, "-"),
        ({}, "-"),
    ],
)
def test_대상키_선정_순서(row, expected):
    assert _subject_key(row) == expected


def test_대상키는_길이를_자른다():
    assert len(_subject_key({"field_path": "a" * 1000})) == 256


def test_first_seen_은_검사당_한_번만_조회한다(conn):
    """위반 하나마다 조회하면 1,000건에서 질의가 1,000번이다."""
    from domains.datacatalog.checks import _first_seen_map

    dag = _seed_dag_run(conn, "dag-fs")
    for field in ("f1", "f2", "f3"):
        _write(conn, dag, subject_key=field, finding="TYPE_CHANGED")

    keys = [("ops.evidence", f, "TYPE_CHANGED") for f in ("f1", "f2", "f3")]
    found = _first_seen_map(conn, "SCHEMA_DRIFT", "03_schema_drift", keys)
    assert set(found) == set(keys)
    assert all(v == dag for v in found.values())


def test_first_seen_은_같은_check_type_의_다른_검사를_섞지_않는다(conn):
    """03·04 는 둘 다 SCHEMA_DRIFT 다. check_name 이 없으면 서로의 이력을 가져온다."""
    from domains.datacatalog.checks import _first_seen_map, _write_result

    dag = _seed_dag_run(conn, "dag-mix")
    _write_result(
        conn, dag_run_id=dag, check_name="04_unversioned_change", check_type="SCHEMA_DRIFT",
        asset_id="ops.evidence", subject_key="f1", status="failed", severity="error",
        finding="TYPE_CHANGED", observed=None, expected=None, checked_at=NOW,
    )
    found = _first_seen_map(
        conn, "SCHEMA_DRIFT", "03_schema_drift", [("ops.evidence", "f1", "TYPE_CHANGED")]
    )
    assert found == {}


def test_중복_검사가_두_단계여도_같은_것을_잡는다(conn):
    """전체 GROUP BY 를 두 단계로 바꿨다. 빨라졌지만 놓치는 게 생기면 안 된다.

    특히 원본이 오래됐고 복제본만 최근인 쌍 — 기간 조건을 GROUP BY 앞으로
    내리면 이걸 놓친다. 두 단계 질의는 후보 키를 먼저 뽑으므로 잡는다.
    """
    from domains.datacatalog.checks import load_sql

    conn.execute(text("DELETE FROM catalog_normalized_evidence WHERE evidence_id LIKE 'dup-%'"))
    rows = [
        ("dup-1", "2026-06-01T00:00:01+00", "2026-07-15T00:00:00+00", "run-a"),
        ("dup-2", "2026-06-01T00:00:07+00", "2026-07-19T00:00:00+00", "run-b"),
        ("dup-3", "2026-06-02T00:00:01+00", "2026-07-19T00:00:00+00", "run-c"),
    ]
    for eid, observed, ingested, run in rows:
        conn.execute(
            text(
                "INSERT INTO catalog_normalized_evidence (evidence_id, asset_id, run_id, "
                "cluster_id, source_id, resource_uid, collection_status, observed_at, ingested_at) "
                "VALUES (:e,'ops.normalized_evidence',:r,'c1','loki','uid-dup','SUCCESS',:o,:i)"
            ),
            {"e": eid, "r": run, "o": observed, "i": ingested},
        )

    found = conn.execute(
        text(load_sql("duplicate_candidates")), {"logical_ts": NOW}
    ).mappings().all()

    # 06-01 은 서로 다른 run 둘이 적재 -> 중복. 06-02 는 run 하나 -> 정상
    assert len(found) == 1
    assert found[0]["resource_uid"] == "uid-dup"
    assert found[0]["run_count"] == 2
    assert str(found[0]["observed_day"]).startswith("2026-06-01")


def test_두_단계_질의는_적재_창_밖의_중복을_놓친다(conn):
    """이건 통과해야 할 동작이 아니라 알고 있는 사각지대다.

    두 단계로 바꾸면서 후보를 "최근 2일 안에 적재된 것"으로 좁혔다. 원본과
    복제본이 둘 다 그보다 오래전에 적재됐으면 후보에 들어오지 않는다.
    전량 GROUP BY 는 잡던 경우다. 1.76배를 얻는 대신 내준 것이 이것이다.

    검사를 매일 돌리는 한 중복이 생긴 날 안에 창 안으로 들어오므로 실제로
    새는 경우는 배치가 이틀 넘게 멈춘 뒤 재개될 때다. 그때는 창을 넓혀
    한 번 돌려야 한다. 사각지대가 좁혀지거나 넓어지면 이 검사가 알려 준다.
    """
    from domains.datacatalog.checks import load_sql

    conn.execute(text("DELETE FROM catalog_normalized_evidence WHERE evidence_id LIKE 'old-%'"))
    # 같은 날 관측, 서로 다른 run. 위 검사와 모양이 같고 다른 것은 적재 시각뿐이다.
    # 관측 시각을 몇 초 어긋나게 두는 이유는 유일 제약이 (cluster_id, source_id,
    # resource_uid, observed_at) 이기 때문이다. 완전히 같은 시각은 DB 가 이미 막는다.
    for eid, run, observed in (
        ("old-1", "run-x", "2026-06-01T00:00:01+00"),
        ("old-2", "run-y", "2026-06-01T00:00:07+00"),
    ):
        conn.execute(
            text(
                "INSERT INTO catalog_normalized_evidence (evidence_id, asset_id, run_id, "
                "cluster_id, source_id, resource_uid, collection_status, observed_at, ingested_at) "
                "VALUES (:e,'ops.normalized_evidence',:r,'c1','loki','uid-old','SUCCESS',"
                ":o,'2026-07-01T00:00:00+00')"
            ),
            {"e": eid, "r": run, "o": observed},
        )

    found = conn.execute(
        text(load_sql("duplicate_candidates")), {"logical_ts": NOW}
    ).mappings().all()

    assert not [r for r in found if r["resource_uid"] == "uid-old"], (
        "적재 창이 넓어졌다면 이 검사와 문서의 한계 서술을 같이 고쳐야 한다"
    )
