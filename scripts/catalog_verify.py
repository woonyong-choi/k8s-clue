#!/usr/bin/env python3
"""README 가 약속한 항목을 실제로 검증한다.

각 항목은 "무엇을 막으려는가"로 짜여 있다. 기능이 도는지가 아니라
사고가 재현되지 않는지를 본다.

마지막 항목이 중요하다. 문제 있는 데이터로만 시험하면
"항상 문제라고 답하는 검사"도 통과한다. 정상 데이터에서 0건이 나오는지
반드시 함께 본다.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sqlalchemy import create_engine, text  # noqa: E402

DSN = os.environ.get(
    "CATALOG_DATABASE_URL", "postgresql+psycopg://postgres@127.0.0.1:5433/catalog"
)
TODAY = os.environ.get("CATALOG_TODAY", "2026-07-31")
RUN = [sys.executable, str(ROOT / "scripts" / "catalog_run.py"), "--today", TODAY]

engine = create_engine(DSN, future=True)
results: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append((label, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f" — {detail}" if detail else ""))


def run(*args: str) -> None:
    subprocess.run(RUN + list(args), check=False, capture_output=True, text=True)


def scalar(sql: str, **params: object) -> object:
    with engine.begin() as conn:
        return conn.execute(text(sql), params).scalar()


def reset() -> None:
    import domains.datacatalog.models  # noqa: F401
    from packages.storage.base import Base

    tables = [t for n, t in Base.metadata.tables.items() if n.startswith("catalog_")]
    Base.metadata.drop_all(engine, tables=tables)
    Base.metadata.create_all(engine, tables=tables)
    archive = ROOT / ".catalog-archive"
    # Airflow와 bind mount로 공유하는 root inode를 지우면 실행 중인
    # container는 삭제된 mount를 계속 보게 된다. root는 유지하고
    # 검증이 만든 날짜별 산출물만 비운다.
    archive.mkdir(parents=True, exist_ok=True)
    for child in archive.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def main() -> int:
    reset()

    # 1. 멱등성 — 같은 논리 날짜를 3회 재실행해도 상태 테이블이 늘지 않는다.
    run("--logical-date", "2026-07-30")
    first = scalar("SELECT count(*) FROM catalog_loads")
    run("--logical-date", "2026-07-30")
    run("--logical-date", "2026-07-30")
    third = scalar("SELECT count(*) FROM catalog_loads")
    check("멱등성: 3회 재실행 후 적재 행 수 불변", first == third, f"{first} → {third}")

    # 이력 테이블은 반대로 늘어야 한다. 하나로 뭉뚱그리면 이력이 사라진다.
    edges = scalar("SELECT count(*) FROM catalog_lineage_edges")
    check("이력 테이블은 유지된다", edges > 0, f"lineage {edges}행")

    # 2. 부분 실패 — 한 소스가 죽어도 나머지는 적재된다.
    reset()
    run("--logical-date", "2026-07-30", "--fail", "loki")
    status = scalar("SELECT status FROM catalog_dag_runs WHERE logical_date = '2026-07-30'")
    loki = scalar(
        "SELECT status FROM catalog_collection_runs WHERE source_id = 'loki'"
    )
    others = scalar(
        "SELECT count(*) FROM catalog_collection_runs "
        "WHERE source_id <> 'loki' AND status = 'SUCCESS'"
    )
    loaded = scalar("SELECT count(*) FROM catalog_loads")
    check("부분 실패: DAG 상태가 PARTIAL", status == "PARTIAL", str(status))
    check("부분 실패: loki 만 FAILED", loki == "FAILED", str(loki))
    check("부분 실패: 나머지 3소스 SUCCESS", others == 3, f"{others}/3")
    check("부분 실패: 나머지 소스가 실제로 적재됨", loaded > 0, f"{loaded}행")

    # 3. 전 소스 실패 — 상태 확정 task 는 그래도 실행된다.
    reset()
    run("--logical-date", "2026-07-30", "--fail", "kubernetes,prometheus,loki,tempo")
    status = scalar("SELECT status FROM catalog_dag_runs WHERE logical_date = '2026-07-30'")
    check("전 소스 실패: FAILED 로 기록", status == "FAILED", str(status))

    # 4. downstream 실패 — 적재 0건인 실행이 성공으로 남지 않는다.
    reset()
    run("--logical-date", "2026-07-30", "--fail-downstream")
    status = scalar("SELECT status FROM catalog_dag_runs WHERE logical_date = '2026-07-30'")
    loaded = scalar("SELECT count(*) FROM catalog_loads")
    check("downstream 실패: INCOMPLETE 로 기록", status == "INCOMPLETE", str(status))
    check("downstream 실패: 적재 0건", loaded == 0, f"{loaded}행")

    # 5. 스키마 드리프트 — 상대가 형식을 바꾸면 알아챈다.
    reset()
    run("--logical-date", "2026-07-30")
    run("--logical-date", "2026-07-31", "--fixture", "drift")
    drift = scalar(
        "SELECT count(*) FROM catalog_quality_results "
        "WHERE check_type = 'SCHEMA_DRIFT' AND status = 'failed'"
    )
    kinds = scalar(
        "SELECT string_agg(DISTINCT finding, ',') FROM catalog_quality_results "
        "WHERE check_type = 'SCHEMA_DRIFT' AND status = 'failed'"
    )
    check("드리프트 검출", drift > 0, f"{drift}건 — {kinds}")

    # 6. 버전을 올리지 않은 변경 — 계약 이력이 append-only 라서 잡힌다.
    unversioned = scalar(
        "SELECT count(*) FROM (SELECT asset_id, schema_version "
        "FROM catalog_schema_observations GROUP BY asset_id, schema_version "
        "HAVING count(DISTINCT schema_hash) > 1) x"
    )
    check("버전 미갱신 변경 검출", unversioned > 0, f"{unversioned}개 자산")

    # 7. 리니지 역추적 — 정규화 행에서 원본 객체까지.
    traced = scalar(
        """
        SELECT count(*) FROM catalog_normalized_evidence e
        JOIN catalog_raw_snapshots s ON s.run_id = e.run_id
        """
    )
    check("리니지 역추적: 정규화 → 실행 → 원본", traced > 0, f"{traced}건")

    # 8. backfill 결정성 — 원본이 있으면 원천을 다시 조회하지 않는다.
    before = scalar(
        "SELECT count(*) FROM catalog_normalized_evidence WHERE observed_at::date = '2026-07-30'"
    )
    run("--logical-date", "2026-07-30")
    after = scalar(
        "SELECT count(*) FROM catalog_normalized_evidence WHERE observed_at::date = '2026-07-30'"
    )
    check("backfill: 재실행해도 관측 행 불변", before == after, f"{before} → {after}")

    # 9. 오탐 방지 — 정상 데이터에서 모든 검사가 0건.
    reset()
    run("--logical-date", "2026-07-30")
    failed = scalar(
        "SELECT count(*) FROM catalog_quality_results WHERE status = 'failed'"
    )
    detail = scalar(
        "SELECT string_agg(check_name || ':' || COALESCE(finding,'-'), ', ') "
        "FROM catalog_quality_results WHERE status = 'failed'"
    )
    check("오탐 방지: 정상 데이터에서 검사 0건", failed == 0, detail or "")

    # 통과 결과도 남는지. 실패만 저장하면 미검사와 통과를 구분할 수 없다.
    passed = scalar("SELECT count(*) FROM catalog_quality_results WHERE status = 'passed'")
    check("통과 결과도 적재된다", passed > 0, f"{passed}건")

    print()
    ok = sum(1 for _, good, _ in results if good)
    print(f"{ok}/{len(results)} 통과")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
