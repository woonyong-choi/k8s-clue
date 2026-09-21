"""카탈로그 조회 API.

설계 근거: docs/catalog-api-mcp.md

모든 응답이 같은 envelope 을 쓴다. data / page / evidence 셋이다.

evidence 가 항상 붙는 이유: run_status 가 PARTIAL 이면 이 조회 결과 자체가
부분 데이터라는 뜻이다. 카탈로그가 "이슈 0건"이라고 답해도 그 검사가 일부
소스를 못 봤다면 0건의 의미가 다르다. 01번 문서의 원칙이 한 단계 위로
올라간다. 수집 결과의 완전성뿐 아니라 검사 결과의 완전성도 전달한다.
"""

from __future__ import annotations

import base64
import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from domains.datacatalog.checks import load_sql
from packages.contracts.catalog.reason_codes import (
    COLLECTION_STATUS_REASONS,
    Reason,
    ReasonCode,
    bound_reasons,
)
from packages.contracts.catalog.vocabulary import (
    DEGRADED_DAG_STATUSES,
    HEALTHY_COLLECTION_STATUSES,
    UNHEALTHY_COLLECTION_STATUSES,
    severity_pattern,
    sql_in_list,
)

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


# 커서 ---------------------------------------------------------------------


def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"offset": offset}).encode()).decode()


def decode_cursor(cursor: str | None) -> int:
    """상한만 두고 페이지네이션이 없으면 상한 너머 데이터에 영원히 접근할 수 없다.

    02번 문서에서 잘림을 숨기지 않기로 했는데, 숨기지 않는 것과
    도달할 수 있게 하는 것은 다르다.
    """
    if not cursor:
        return 0
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        offset = int(payload["offset"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(422, {"code": "invalid_parameter", "field": "cursor"}) from exc
    if offset < 0:
        raise HTTPException(422, {"code": "invalid_parameter", "field": "cursor"})
    return offset


# envelope ------------------------------------------------------------------


def latest_evidence(conn: Connection) -> dict[str, Any]:
    """가장 최근 검사 실행의 근거.

    검사가 한 번도 돌지 않았으면 NEVER_RUN 이다. "아직 검사 안 됨"과
    "검사했는데 이슈 없음"을 같은 응답으로 내보내지 않는다.
    """
    row = conn.execute(
        text(
            """
            SELECT dag_run_id, logical_date, status, finished_at
            FROM catalog_dag_runs
            ORDER BY logical_date DESC, started_at DESC
            LIMIT 1
            """
        )
    ).mappings().first()

    if row is None:
        codes, truncated = bound_reasons([Reason(ReasonCode.NEVER_RUN)])
        return {
            "run_id": None,
            "logical_date": None,
            "run_status": "NEVER_RUN",
            "checked_at": None,
            "reason_codes": codes,
            "reason_codes_truncated": truncated,
        }

    reasons: list[Reason] = []
    if row["status"] in DEGRADED_DAG_STATUSES:
        # 상태 목록을 질의에 직접 적으면 어휘가 늘 때 여기만 옛 목록으로 남는다.
        # 사유가 남은 행도 같이 본다 — NO_DATA 인데 과거 원본이 없어 복원을
        # 포기한 경우가 그렇다. 상태는 정상 범위지만 그날은 비어 있다.
        degraded = conn.execute(
            text(
                "SELECT source_id, status FROM catalog_collection_runs "
                f"WHERE dag_run_id = :d AND status IN ({sql_in_list(UNHEALTHY_COLLECTION_STATUSES)})"
            ),
            {"d": row["dag_run_id"]},
        ).mappings().all()
        reasons = [
            Reason(COLLECTION_STATUS_REASONS[str(r["status"])], source=str(r["source_id"]))
            for r in degraded
        ]

    codes, truncated = bound_reasons(reasons)
    return {
        "run_id": row["dag_run_id"],
        "logical_date": str(row["logical_date"]),
        "run_status": row["status"],
        "checked_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "reason_codes": codes,
        "reason_codes_truncated": truncated,
    }


def envelope(
    conn: Connection, rows: list[dict[str, Any]], *, limit: int, offset: int, total: int
) -> dict[str, Any]:
    returned = len(rows)
    truncated = offset + returned < total
    page: dict[str, Any] = {
        "limit": limit,
        "returned_count": returned,
        "total_estimated": total,
        "truncated": truncated,
    }
    if truncated:
        page["next_cursor"] = encode_cursor(offset + returned)
    return {"data": rows, "page": page, "evidence": latest_evidence(conn)}


# 의존성 --------------------------------------------------------------------


def get_connection() -> Connection:  # pragma: no cover - 앱 배선에서 주입된다
    raise NotImplementedError("애플리케이션 배선에서 오버라이드한다")


def require_read_scope(
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    """읽기 권한을 확인한다.

    MCP 가 권한을 좁힌 토큰을 보내도, 받는 쪽이 검사하지 않으면 좁힌 의미가 없다.
    "토큰을 보냈다" 와 "토큰으로 판정했다" 는 다르고, 후자가 없으면 전자는 장식이다.

    검증기는 배선에서 주입한다. 여기서 JWT 를 직접 파싱하면 이 라우터가 인증
    방식에 묶이고, 방식이 바뀔 때마다 조회 코드를 고쳐야 한다.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"code": "missing_token"})
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail={"code": "missing_token"})
    return token


Conn = Annotated[Connection, Depends(get_connection)]
Principal = Annotated[str, Depends(require_read_scope)]
Limit = Annotated[int, Query(ge=1, le=MAX_LIMIT)]


# 엔드포인트 ----------------------------------------------------------------


@router.get("/sources")
def list_sources(principal: Principal,
    conn: Conn, limit: Limit = DEFAULT_LIMIT, cursor: str | None = None):
    offset = decode_cursor(cursor)
    total = conn.execute(text("SELECT count(*) FROM catalog_data_sources")).scalar_one()
    rows = conn.execute(
        text(
            "SELECT source_id, name, source_type, owner, enabled, "
            "       collection_interval_seconds "
            "FROM catalog_data_sources ORDER BY source_id LIMIT :l OFFSET :o"
        ),
        {"l": limit, "o": offset},
    ).mappings().all()
    return envelope(conn, [dict(r) for r in rows], limit=limit, offset=offset, total=total)


@router.get("/assets")
def search_assets(
    principal: Principal,
    conn: Conn,
    q: str | None = Query(None, max_length=128),
    source: str | None = Query(None, max_length=64),
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
):
    """자산 검색.

    classification 을 필터로 노출하지만 인가 입력으로는 쓰지 않는다.
    그 한계는 07번 문서에 적어 두었다.
    """
    offset = decode_cursor(cursor)
    # 바인드에 타입을 준다. 없으면 Postgres 가 파라미터 타입을 정하지 못해
    # 이 엔드포인트가 어떤 입력에도 500 이 된다.
    where = "WHERE (CAST(:q AS text) IS NULL OR qualified_name ILIKE '%' || :q || '%') " \
            "AND (CAST(:s AS text) IS NULL OR source_id = :s)"
    params = {"q": q, "s": source, "l": limit, "o": offset}
    total = conn.execute(
        text(f"SELECT count(*) FROM catalog_data_assets {where}"), params
    ).scalar_one()
    rows = conn.execute(
        text(
            f"SELECT asset_id, qualified_name, asset_type, source_id, owner, "
            f"       classification, current_schema_version, freshness_sla_seconds "
            f"FROM catalog_data_assets {where} ORDER BY qualified_name LIMIT :l OFFSET :o"
        ),
        params,
    ).mappings().all()
    return envelope(conn, [dict(r) for r in rows], limit=limit, offset=offset, total=total)


@router.get("/assets/{asset_id}")
def get_asset(principal: Principal,
    conn: Conn, asset_id: Annotated[str, Path(max_length=256)]):
    row = conn.execute(
        text("SELECT * FROM catalog_data_assets WHERE asset_id = :a"), {"a": asset_id}
    ).mappings().first()
    if row is None:
        raise HTTPException(404, {"code": "not_found"})
    return envelope(conn, [dict(row)], limit=1, offset=0, total=1)


@router.get("/assets/{asset_id}/schema")
def get_asset_schema(principal: Principal,
    conn: Conn, asset_id: Annotated[str, Path(max_length=256)]):
    """계약 이력.

    append-only 인 schema_observations 를 읽는다. asset_fields 는 upsert 되므로
    이전 세대 해시가 남지 않는다.
    """
    rows = conn.execute(
        text(
            """
            SELECT schema_version, schema_hash, first_seen_run_id, first_seen_at
            FROM catalog_schema_observations
            WHERE asset_id = :a
            ORDER BY first_seen_at
            """
        ),
        {"a": asset_id},
    ).mappings().all()
    if not rows:
        raise HTTPException(404, {"code": "not_found"})
    data = [
        {
            "schema_version": r["schema_version"],
            "schema_hash": r["schema_hash"],
            "first_seen_run_id": r["first_seen_run_id"],
            "first_seen_at": r["first_seen_at"].isoformat(),
        }
        for r in rows
    ]
    return envelope(conn, data, limit=len(data), offset=0, total=len(data))


@router.get("/assets/{asset_id}/lineage")
def get_asset_lineage(principal: Principal,
    conn: Conn, asset_id: Annotated[str, Path(max_length=256)]):
    """리니지 역추적.

    간선이 언제 확인됐는지 함께 반환한다. run_id 를 저장만 하고 조인하지
    않으면 "이 관계가 언제 확인된 것인가"에 답할 수 없다.
    """
    rows = conn.execute(
        text(load_sql("lineage_trace")), {"asset_id": asset_id, "logical_ts": _now(conn)}
    ).mappings().all()
    data = [dict(r) for r in rows]
    return envelope(conn, data, limit=len(data), offset=0, total=len(data))


@router.get("/resources/state")
def list_resource_state(
    principal: Principal,
    conn: Conn,
    cluster_id: Annotated[str, Query(max_length=128)],
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
):
    """리소스별 마지막 관측 상태.

    "이 리소스 지금 믿어도 되나"에 답한다. 자산 단위 최신성(05번 검사)과 다르다 —
    자산이 살아 있어도 그 안의 리소스 하나가 며칠째 안 들어올 수 있다.

    최신 관측이 불완전하면 그 사실을 함께 준다. 완전한 것만 골라 주면 더 새로운
    관측이 있었다는 사실이 사라진다.
    """
    offset = decode_cursor(cursor)
    rows = conn.execute(
        text(load_sql("latest_state")), {"cluster_id": cluster_id, "logical_ts": _now(conn)}
    ).mappings().all()
    data = [
        {**dict(r), "observed_at": r["observed_at"].isoformat()}
        for r in rows[offset : offset + limit]
    ]
    return envelope(conn, data, limit=limit, offset=offset, total=len(rows))


@router.get("/quality/issues")
def list_quality_issues(
    principal: Principal,
    conn: Conn,
    severity: str | None = Query(None, pattern=severity_pattern()),
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
):
    offset = decode_cursor(cursor)
    # "미해결"은 최신 실행에서 여전히 실패인 것이다. 실행 전체를 대상으로 하면
    # 6월에 한 번 실패한 결과가 오늘도 미해결로 나오고, 실행이 30번 돌면 같은
    # 이슈가 30행 반환된다. 해소 여부를 판정하지 않는 목록은 이슈 목록이 아니다.
    where = (
        "WHERE status = 'failed' AND (CAST(:sev AS text) IS NULL OR severity = :sev) "
        "AND dag_run_id = (SELECT dag_run_id FROM catalog_dag_runs "
        "                  ORDER BY logical_date DESC, started_at DESC LIMIT 1)"
    )
    params = {"sev": severity, "l": limit, "o": offset}
    total = conn.execute(
        text(f"SELECT count(*) FROM catalog_quality_results {where}"), params
    ).scalar_one()
    rows = conn.execute(
        text(
            f"SELECT result_id, check_name, check_type, asset_id, subject_key, severity, finding, "
            f"       observed_value, expected_value, first_seen_dag_run_id, checked_at "
            f"FROM catalog_quality_results {where} "
            f"ORDER BY severity, checked_at DESC LIMIT :l OFFSET :o"
        ),
        params,
    ).mappings().all()
    data = [{**dict(r), "checked_at": r["checked_at"].isoformat()} for r in rows]
    return envelope(conn, data, limit=limit, offset=offset, total=total)


@router.get("/runs")
def list_runs(
    principal: Principal,
    conn: Conn,
    logical_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
):
    """실행 이력과 소스별 지표.

    별도 지표 저장소를 두지 않았다. 실행 이력이 이미 지표의 원천이다.
    """
    offset = decode_cursor(cursor)
    date_filter = "WHERE (CAST(:d AS text) IS NULL OR d.logical_date = CAST(:d AS date))"
    total = conn.execute(
        text(
            "SELECT count(*) FROM catalog_dag_runs d "
            + date_filter
        ),
        {"d": logical_date},
    ).scalar_one()
    rows = conn.execute(
        text(
            f"""
            SELECT d.dag_run_id, d.logical_date, d.status, d.started_at, d.finished_at,
                   count(c.run_id) AS source_count,
                   count(*) FILTER (
                       WHERE c.status IN ({sql_in_list(HEALTHY_COLLECTION_STATUSES)})
                   ) AS healthy,
                   count(*) FILTER (
                       WHERE c.status IN ({sql_in_list(UNHEALTHY_COLLECTION_STATUSES)})
                   ) AS unhealthy,
                   sum(c.attempt) AS attempts
            FROM catalog_dag_runs d
            LEFT JOIN catalog_collection_runs c ON c.dag_run_id = d.dag_run_id
            WHERE (CAST(:d AS text) IS NULL OR d.logical_date = CAST(:d AS date))
            GROUP BY d.dag_run_id, d.logical_date, d.status, d.started_at, d.finished_at
            ORDER BY d.logical_date DESC LIMIT :l OFFSET :o
            """
        ),
        {"l": limit, "o": offset, "d": logical_date},
    ).mappings().all()
    data = [
        {
            **dict(r),
            "logical_date": str(r["logical_date"]),
            "started_at": r["started_at"].isoformat(),
            "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
        }
        for r in rows
    ]
    return envelope(conn, data, limit=limit, offset=offset, total=total)


def _now(conn: Connection):
    return conn.execute(text("SELECT now()")).scalar_one()
