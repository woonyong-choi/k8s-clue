"""정합성 검사 실행기.

sql/checks/ 와 sql/lookups/ 의 질의를 읽어 실행하고 결과를 catalog_quality_results 에 적재한다.
검사 기준을 Python 이 아니라 SQL 에 둔 이유는 06번 문서에 있다.

Python 은 질의를 실행하고 결과를 적재하는 역할만 한다.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from packages.contracts.catalog.vocabulary import CHECK_SEVERITY, CHECK_TYPES

SQL_ROOT = Path(__file__).resolve().parents[3] / "sql"
CHECK_DIR = SQL_ROOT / "checks"
LOOKUP_DIR = SQL_ROOT / "lookups"

# 검사 파일과 검사 종류의 대응.
#
# 파일 이름에 번호를 붙이지 않는다. 검사끼리 의존이 없어 순서가 바뀌어도 결과가
# 같은데, 번호는 순서가 있다고 읽히게 만든다. 검사와 조회는 디렉터리로 나눈다.
CHECK_FILES: dict[str, str] = {
    "source_coverage": "SOURCE_COVERAGE",
    "required_field": "REQUIRED_FIELD",
    "schema_drift": "SCHEMA_DRIFT",
    "unversioned_change": "SCHEMA_DRIFT",
    "freshness": "FRESHNESS",
    "lineage_break": "LINEAGE_BREAK",
    "run_consistency": "RUN_CONSISTENCY",
    "duplicate_candidates": "RUN_CONSISTENCY",
}

LOOKUP_FILES = ("latest_state", "lineage_trace")

# 검사 종류를 계약에 없는 값으로 적으면 심각도 조회에서 KeyError 가 나는데,
# 그건 배치가 돌기 시작한 뒤다. 임포트 시점에 걸러 낸다.
assert set(CHECK_FILES.values()) <= set(CHECK_TYPES), (
    f"계약에 없는 검사 종류: {sorted(set(CHECK_FILES.values()) - set(CHECK_TYPES))}"
)


def load_sql(name: str) -> str:
    directory = LOOKUP_DIR if name in LOOKUP_FILES else CHECK_DIR
    return (directory / f"{name}.sql").read_text(encoding="utf-8")


def _bind_params(sql: str, params: dict[str, Any]) -> dict[str, Any]:
    """질의가 실제로 쓰는 바인드만 넘긴다."""
    return {k: v for k, v in params.items() if f":{k}" in sql}


def run_checks(
    conn: Connection,
    dag_run_id: str,
    logical_date: str,
    logical_ts: datetime,
    checked_at: datetime,
) -> list[dict[str, Any]]:
    """검사 8개를 실행하고 결과를 적재한다.

    위반이 0건이어도 통과 결과를 남긴다. 실패만 저장하면 "검사를 안 한 것"과
    "검사했는데 통과한 것"을 구분할 수 없다. 01번 문서의 빈 목록 문제와 같다.
    """
    params = {
        "logical_date": logical_date,
        "logical_ts": logical_ts,
        "run_id": None,  # 03번 검사가 쓰는 소스별 run_id 는 아래에서 채운다
    }
    findings: list[dict[str, Any]] = []

    for name, check_type in CHECK_FILES.items():
        sql = load_sql(name)
        severity = CHECK_SEVERITY[check_type]

        if ":run_id" in sql:
            run_ids = conn.execute(
                text("SELECT run_id FROM catalog_collection_runs WHERE dag_run_id = :d"),
                {"d": dag_run_id},
            ).scalars().all()
            rows: list[Any] = []
            for run_id in run_ids:
                rows.extend(
                    conn.execute(
                        text(sql), _bind_params(sql, {**params, "run_id": run_id})
                    ).mappings().all()
                )
        else:
            rows = list(
                conn.execute(text(sql), _bind_params(sql, params)).mappings().all()
            )

        if not rows:
            _write_result(
                conn,
                dag_run_id=dag_run_id,
                check_name=name,
                check_type=check_type,
                asset_id="-",
                subject_key="-",
                status="passed",
                severity=severity,
                finding=None,
                observed=None,
                expected=None,
                checked_at=checked_at,
            )
            continue

        # 위반 하나마다 first_seen 을 조회하면 N+1 이 된다. 위반이 1,000건이면
        # 질의가 1,000번이다. 검사 단위로 한 번에 읽어 사전으로 들고 쓴다.
        keys = [
            (
                str(row.get("asset_id") or row.get("source_id") or row.get("dag_run_id")),
                _subject_key(row),
                str(row.get("finding") or row.get("drift_type") or check_type),
            )
            for row in rows
        ]
        seen_before = _first_seen_map(conn, check_type, name, keys)

        for row in rows:
            asset_id = row.get("asset_id") or row.get("source_id") or row.get("dag_run_id")
            finding = row.get("finding") or row.get("drift_type") or check_type
            subject_key = _subject_key(row)
            first_seen = seen_before.get(
                (str(asset_id), subject_key, str(finding)), dag_run_id
            )
            _write_result(
                conn,
                dag_run_id=dag_run_id,
                check_name=name,
                check_type=check_type,
                asset_id=str(asset_id) if asset_id is not None else "-",
                subject_key=subject_key,
                status="failed",
                severity=severity,
                finding=str(finding),
                observed=_stringify(row, ("observed_type", "observed_value", "staleness_seconds",
                                          "detail", "observation_count", "run_count", "duplicate_count",
                                          "hash_variants", "violation_count")),
                expected=_stringify(row, ("declared_type", "expected_value",
                                          "freshness_sla_seconds")),
                checked_at=checked_at,
                first_seen=first_seen,
            )
            findings.append(dict(row))

    return findings


def _subject_key(row: Any) -> str:
    """한 자산 안에서 위반을 구분하는 값.

    03 은 필드마다, 02 는 누락 필드마다, 08 은 리소스마다 위반을 낸다.
    이 값이 없으면 유일 제약이 자산 단위라 두 번째 위반부터 앞의 것을 덮어쓴다.
    """
    parts = [
        str(row[key])
        for key in ("cluster_id", "observed_day", "upstream_asset_id", "downstream_asset_id",
                    "field_path", "resource_uid", "schema_version", "detail", "qualified_name")
        if row.get(key) is not None
    ]
    return "/".join(parts)[:256] if parts else "-"


def _stringify(row: Any, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


def _first_seen_map(
    conn: Connection,
    check_type: str,
    check_name: str,
    keys: list[tuple[str, str, str]],
) -> dict[tuple[str, str, str], str]:
    """이 위반들이 처음 발생한 실행을 한 번에 찾는다.

    이 값이 없으면 한 번 발생한 영구 위반이 이후 모든 실행을 정합성 위반으로
    만든다. 일주일이면 전부 붉어지고 그 뒤로는 아무도 안 본다.

    check_name 까지 거르는 이유: 03·04 는 둘 다 SCHEMA_DRIFT 라 check_type
    만으로는 서로의 이력을 가져온다.
    """
    if not keys:
        return {}
    rows = conn.execute(
        text(
            """
            SELECT asset_id, subject_key, finding,
                   COALESCE(MIN(first_seen_dag_run_id), MIN(dag_run_id)) AS first_seen
            FROM catalog_quality_results
            WHERE check_type = :c AND check_name = :cn AND status = 'failed'
              AND asset_id = ANY(:assets)
            GROUP BY asset_id, subject_key, finding
            """
        ),
        {"c": check_type, "cn": check_name, "assets": sorted({k[0] for k in keys})},
    ).mappings().all()
    return {
        (r["asset_id"], r["subject_key"], str(r["finding"])): r["first_seen"] for r in rows
    }


def _write_result(
    conn: Connection,
    *,
    dag_run_id: str,
    check_name: str,
    check_type: str,
    asset_id: str,
    subject_key: str,
    status: str,
    severity: str,
    finding: str | None,
    observed: str | None,
    expected: str | None,
    checked_at: datetime,
    first_seen: str | None = None,
) -> None:
    key = f"{dag_run_id}/{check_name}/{asset_id}/{subject_key}"
    conn.execute(
        text(
            """
            INSERT INTO catalog_quality_results
                (result_id, dag_run_id, check_name, check_type, asset_id, subject_key,
                 status, severity, finding, observed_value, expected_value,
                 first_seen_dag_run_id, checked_at)
            VALUES (:rid, :d, :cn, :c, :a, :sk, :s, :sev, :f, :obs, :exp, :first, :t)
            ON CONFLICT (dag_run_id, check_name, asset_id, subject_key) DO UPDATE
                SET status = EXCLUDED.status,
                    severity = EXCLUDED.severity,
                    finding = EXCLUDED.finding,
                    observed_value = EXCLUDED.observed_value,
                    expected_value = EXCLUDED.expected_value,
                    checked_at = EXCLUDED.checked_at
            """
        ),
        {
            "rid": f"qr-{uuid.uuid5(uuid.NAMESPACE_URL, key)}",
            "d": dag_run_id,
            "cn": check_name,
            "c": check_type,
            "a": asset_id,
            "sk": subject_key,
            "s": status,
            "sev": severity,
            "f": finding,
            "obs": observed,
            "exp": expected,
            "first": first_seen or dag_run_id,
            "t": checked_at,
        },
    )
