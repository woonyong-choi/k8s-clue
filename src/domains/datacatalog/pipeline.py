"""배치 파이프라인 단계 구현.

DAG 는 이 함수들을 호출하기만 한다. Airflow 없이도 테스트할 수 있도록
오케스트레이션과 로직을 분리했다.

설계 근거는 docs/05-airflow-pipeline.md 에 있다.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

if TYPE_CHECKING:
    from domains.datacatalog.sources import CatalogSource

from domains.datacatalog.models import (
    HEALTHY_COLLECTION_STATUSES,
    UNHEALTHY_COLLECTION_STATUSES,
    collection_run_id_for,
    dag_run_id_for,
)
from domains.datacatalog.schema_contract import (
    contract_from_payload,
    schema_hash,
)

DAG_ID = "catalog_reconciliation_daily"


@dataclass
class ExtractOutcome:
    """소스 하나의 추출 결과."""

    source_id: str
    status: str
    payloads: list[dict[str, Any]] = field(default_factory=list)
    s3_uri: str | None = None
    content_hash: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in HEALTHY_COLLECTION_STATUSES


# 1. extract --------------------------------------------------------------


def extract_source(
    source_id: str,
    logical_date: str,
    source: CatalogSource,
    archive_root: Path,
    *,
    today: str,
    fail_sources: frozenset[str] = frozenset(),
) -> ExtractOutcome:
    """소스 하나에서 추출한다.

    과거 날짜에 원본이 이미 있으면 원천을 건드리지 않고 S3(로컬은 파일)에서
    재생한다. backfill 이 원천을 다시 조회하면, 원천은 과거 상태를 갖고
    있지 않으므로 현재 상태에 과거 날짜 도장을 찍게 된다.

    보관 원본이 없으면 원천에 묻는다. 지금 쓰는 두 원천은 둘 다 저장된 것을
    읽으므로 원천 API 를 다시 때리지는 않는다. 날짜를 구분하는지는 원천마다
    다르고, 그 차이는 아래 주석에 적었다.
    """
    if source_id in fail_sources:
        return ExtractOutcome(source_id=source_id, status="FAILED")

    archived = archive_root / logical_date / f"{source_id}.json"
    if logical_date < today and archived.exists():
        # 재처리. 원천을 건드리지 않고 보관된 원본에서 재생한다.
        # 이 분기가 backfill 을 결정적으로 만든다. 같은 구간을 다른 날
        # 다시 돌려도 같은 입력에서 시작한다.
        payloads = json.loads(archived.read_text(encoding="utf-8"))
        return ExtractOutcome(
            source_id=source_id,
            status="SUCCESS" if payloads else "NO_DATA",
            payloads=payloads,
            s3_uri=f"file://{archived}",
            content_hash=_digest(payloads),
        )

    # 보관 원본이 없으면 원천에서 가져온다. 과거 날짜라도 최초 실행이면 이 경로다.
    # CollectedSource 는 질의에 logical_date 가 들어가므로 과거를 물으면 과거가
    # 온다. FixtureSource 는 날짜를 구분하지 않는 고정 파일이라 같은 값을 준다 —
    # 로컬 재현을 결정적으로 만들기 위한 것이고, 그래서 시연 결과를 원천의
    # 과거 상태로 읽어서는 안 된다.
    payloads = source.fetch(source_id, logical_date)
    return ExtractOutcome(
        source_id=source_id,
        status="SUCCESS" if payloads else "NO_DATA",
        payloads=payloads,
    )


def _digest(payloads: list[dict[str, Any]]) -> str:
    canonical = json.dumps(payloads, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_archived_outcomes(conn: Connection, dag_run_id: str) -> list[ExtractOutcome]:
    """Load task outputs from the raw archive instead of querying sources again.

    Airflow mapped extract tasks persist status and archive raw payloads. Downstream
    normalization must consume that exact snapshot. Calling ``extract_source`` again
    would silently turn one logical run into two source reads and could mix time windows.
    """
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (cr.source_id)
                   cr.source_id, cr.status, rs.s3_uri, rs.content_hash
            FROM catalog_collection_runs AS cr
            LEFT JOIN catalog_raw_snapshots AS rs ON rs.run_id = cr.run_id
            WHERE cr.dag_run_id = :dag_run_id
            ORDER BY cr.source_id, rs.created_at DESC NULLS LAST
            """
        ),
        {"dag_run_id": dag_run_id},
    ).mappings()

    outcomes: list[ExtractOutcome] = []
    for row in rows:
        uri = row["s3_uri"]
        payloads: list[dict[str, Any]] = []
        if uri:
            if not str(uri).startswith("file://"):
                raise ValueError(f"unsupported archive URI: {uri}")
            path = Path(str(uri).removeprefix("file://"))
            payloads = json.loads(path.read_text(encoding="utf-8"))
        outcomes.append(
            ExtractOutcome(
                source_id=str(row["source_id"]),
                status=str(row["status"]),
                payloads=payloads,
                s3_uri=str(uri) if uri else None,
                content_hash=str(row["content_hash"]) if row["content_hash"] else None,
            )
        )
    return outcomes


# 2. archive --------------------------------------------------------------


def archive_raw_snapshot(
    conn: Connection,
    dag_run_id: str,
    logical_date: str,
    outcomes: list[ExtractOutcome],
    archive_root: Path,
) -> None:
    """원본을 보관한다.

    최소 하나가 성공했는지는 호출하는 쪽(DAG task 본문)이 판단한다.
    trigger_rule 이름에 조건을 맡기지 않는 이유는 05번 문서에 있다.

    content_hash 로 객체는 중복 제거하되 행은 실행마다 남긴다.
    행까지 중복 제거하면 이틀 연속 같은 내용일 때 둘째 날의 역추적이 끊긴다.
    """
    day_dir = archive_root / logical_date
    day_dir.mkdir(parents=True, exist_ok=True)

    for outcome in outcomes:
        if not outcome.ok or not outcome.payloads:
            continue
        digest = outcome.content_hash or _digest(outcome.payloads)
        target = day_dir / f"{outcome.source_id}.json"
        if not target.exists():
            target.write_text(
                json.dumps(outcome.payloads, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        outcome.s3_uri = f"file://{target}"
        outcome.content_hash = digest

        run_id = collection_run_id_for(dag_run_id, outcome.source_id)
        conn.execute(
            text(
                """
                INSERT INTO catalog_raw_snapshots
                    (snapshot_id, run_id, s3_uri, content_hash, byte_size)
                VALUES (:sid, :run_id, :uri, :hash, :size)
                ON CONFLICT (run_id, content_hash) DO NOTHING
                """
            ),
            {
                "sid": f"snap-{uuid.uuid5(uuid.NAMESPACE_URL, run_id + digest)}",
                "run_id": run_id,
                "uri": outcome.s3_uri,
                "hash": digest,
                "size": target.stat().st_size,
            },
        )


# 3. normalize ------------------------------------------------------------


def normalize_asset_schema(
    conn: Connection,
    dag_run_id: str,
    logical_date: str,
    outcomes: list[ExtractOutcome],
    observed_at: datetime,
) -> None:
    """실제 payload 에서 필드와 타입을 추출해 관측 테이블에 적재한다."""
    for outcome in outcomes:
        if not outcome.payloads:
            continue
        run_id = collection_run_id_for(dag_run_id, outcome.source_id)
        for item in outcome.payloads:
            asset_id = item["asset_id"]
            row_key = item["row_key"]
            row_id = f"row-{uuid.uuid5(uuid.NAMESPACE_URL, f'{run_id}/{asset_id}/{row_key}')}"

            conn.execute(
                text(
                    """
                    INSERT INTO catalog_observed_rows
                        (row_id, run_id, asset_id, row_key, observed_at)
                    VALUES (:row_id, :run_id, :asset_id, :row_key, :observed_at)
                    ON CONFLICT (run_id, asset_id, row_key) DO UPDATE
                        SET observed_at = EXCLUDED.observed_at
                    """
                ),
                {
                    "row_id": row_id,
                    "run_id": run_id,
                    "asset_id": asset_id,
                    "row_key": row_key,
                    "observed_at": observed_at,
                },
            )

            for path, dtype in contract_from_payload(item["payload"]):
                conn.execute(
                    text(
                        """
                        INSERT INTO catalog_observed_fields
                            (row_id, run_id, asset_id, field_path, data_type)
                        VALUES (:row_id, :run_id, :asset_id, :path, :dtype)
                        ON CONFLICT (row_id, field_path) DO UPDATE
                            SET data_type = EXCLUDED.data_type
                        """
                    ),
                    {
                        "row_id": row_id,
                        "run_id": run_id,
                        "asset_id": asset_id,
                        "path": path,
                        "dtype": dtype,
                    },
                )

            # 정규화 결과. 유일 제약이 backfill 중복을 막는다.
            conn.execute(
                text(
                    """
                    INSERT INTO catalog_normalized_evidence
                        (evidence_id, asset_id, run_id, cluster_id, source_id,
                         resource_uid, collection_status, observed_at, ingested_at)
                    VALUES (:eid, :asset_id, :run_id, :cluster, :source_id,
                            :uid, :status, :observed_at, :ingested_at)
                    ON CONFLICT (cluster_id, source_id, resource_uid, observed_at)
                        DO NOTHING
                    """
                ),
                {
                    "eid": f"ev-{uuid.uuid5(uuid.NAMESPACE_URL, f'{asset_id}/{row_key}/{observed_at}')}",
                    "asset_id": asset_id,
                    "run_id": run_id,
                    "cluster": item.get("cluster_id", "local"),
                    "source_id": outcome.source_id,
                    "uid": row_key,
                    "status": outcome.status,
                    "observed_at": observed_at,
                    "ingested_at": datetime.now(UTC),
                },
            )


# 4. drift ----------------------------------------------------------------


def record_schema_observations(
    conn: Connection, dag_run_id: str, outcomes: list[ExtractOutcome], observed_at: datetime
) -> None:
    """관측된 계약을 append-only 이력에 남긴다.

    같은 계약이 반복 관측되면 행이 늘지 않는다. 계약이 바뀌면 행이 하나 생기고,
    그때 (asset, version) 에 해시가 둘이 되어 04번 검사가 발화한다.
    """
    for outcome in outcomes:
        if not outcome.payloads:
            continue
        run_id = collection_run_id_for(dag_run_id, outcome.source_id)
        by_asset: dict[str, dict[str, str | None]] = {}
        for item in outcome.payloads:
            merged = by_asset.setdefault(item["asset_id"], {})
            for path, dtype in contract_from_payload(item["payload"]):
                if path not in merged or merged[path] is None:
                    merged[path] = dtype

        for asset_id, contract in by_asset.items():
            version = conn.execute(
                text(
                    "SELECT current_schema_version FROM catalog_data_assets "
                    "WHERE asset_id = :a"
                ),
                {"a": asset_id},
            ).scalar()
            if version is None:
                continue
            digest = schema_hash(sorted(contract.items()))
            conn.execute(
                text(
                    """
                    INSERT INTO catalog_schema_observations
                        (asset_id, schema_version, schema_hash, first_seen_run_id, first_seen_at)
                    VALUES (:a, :v, :h, :run_id, :seen)
                    ON CONFLICT (asset_id, schema_version, schema_hash) DO NOTHING
                    """
                ),
                {"a": asset_id, "v": version, "h": digest, "run_id": run_id, "seen": observed_at},
            )


# 5. load -----------------------------------------------------------------


def load_catalog(
    conn: Connection, logical_date: str, outcomes: list[ExtractOutcome], loaded_at: datetime
) -> None:
    """멱등 적재.

    멱등 키에 run_id 를 넣지 않는다. run_id 는 재시도마다 새로 생기므로
    키에 포함하면 재실행 때마다 행이 늘어난다.
    """
    for outcome in outcomes:
        counts: dict[str, int] = {}
        for item in outcome.payloads:
            counts[item["asset_id"]] = counts.get(item["asset_id"], 0) + 1
        for asset_id, count in counts.items():
            conn.execute(
                text(
                    """
                    INSERT INTO catalog_loads
                        (logical_date, source_id, asset_id, row_count, loaded_at)
                    VALUES (:d, :s, :a, :n, :t)
                    ON CONFLICT (logical_date, source_id, asset_id) DO UPDATE
                        SET row_count = EXCLUDED.row_count, loaded_at = EXCLUDED.loaded_at
                    """
                ),
                {"d": logical_date, "s": outcome.source_id, "a": asset_id, "n": count, "t": loaded_at},
            )


def record_lineage(
    conn: Connection, dag_run_id: str, outcomes: list[ExtractOutcome]
) -> None:
    """원본 → 정규화 간선을 실행 단위로 남긴다."""
    for outcome in outcomes:
        if not outcome.ok:
            continue
        run_id = collection_run_id_for(dag_run_id, outcome.source_id)
        for asset_id in {item["asset_id"] for item in outcome.payloads}:
            conn.execute(
                text(
                    """
                    INSERT INTO catalog_lineage_edges
                        (upstream_asset_id, downstream_asset_id, transformation, run_id)
                    VALUES (:up, :down, :tr, :run_id)
                    ON CONFLICT (upstream_asset_id, downstream_asset_id, run_id) DO NOTHING
                    """
                ),
                {
                    "up": asset_id,
                    "down": "ops.normalized_evidence",
                    "tr": "normalize_asset_schema",
                    "run_id": run_id,
                },
            )


# 6. 상태 확정 ------------------------------------------------------------


def upsert_collection_runs(
    conn: Connection,
    dag_run_id: str,
    logical_date: str,
    outcomes: list[ExtractOutcome],
    finished_at: datetime,
) -> None:
    """소스별 실행 상태를 기록한다. 재시도는 같은 행을 갱신하고 attempt 만 올린다."""
    for outcome in outcomes:
        run_id = collection_run_id_for(dag_run_id, outcome.source_id)
        conn.execute(
            text(
                """
                INSERT INTO catalog_collection_runs
                    (run_id, dag_run_id, source_id, logical_date, status, attempt, finished_at)
                VALUES (:run_id, :dag_run_id, :source_id, :d, :status, 1, :finished_at)
                ON CONFLICT (dag_run_id, source_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        attempt = catalog_collection_runs.attempt + 1,
                        finished_at = EXCLUDED.finished_at
                """
            ),
            {
                "run_id": run_id,
                "dag_run_id": dag_run_id,
                "source_id": outcome.source_id,
                "d": logical_date,
                "status": outcome.status,
                "finished_at": finished_at,
            },
        )


def resolve_dag_run_status(
    conn: Connection, dag_run_id: str, *, downstream_complete: bool, finished_at: datetime
) -> str:
    """DAG 실행 상태를 확정한다.

    extract 상태만 읽으면 안 된다. 그러면 validate 가 실패해서 아무것도
    적재하지 않은 실행이 SUCCESS 로 남는다. 그게 이 프로젝트가 없애려는
    바로 그 상태다. downstream 완료 여부를 함께 본다.
    """
    rows = conn.execute(
        text("SELECT status FROM catalog_collection_runs WHERE dag_run_id = :d"),
        {"d": dag_run_id},
    ).scalars().all()

    if not rows:
        status = "FAILED"
    elif all(r == "FAILED" for r in rows):
        status = "FAILED"
    elif not downstream_complete:
        status = "INCOMPLETE"
    # 목록을 여기 다시 적으면 상태가 하나 늘 때 이 판정만 옛 목록으로 남는다.
    elif any(r in UNHEALTHY_COLLECTION_STATUSES for r in rows):
        status = "PARTIAL"
    else:
        status = "SUCCESS"

    conn.execute(
        text(
            "UPDATE catalog_dag_runs SET status = :s, finished_at = :f WHERE dag_run_id = :d"
        ),
        {"s": status, "f": finished_at, "d": dag_run_id},
    )
    return status


def open_dag_run(
    conn: Connection, logical_date: str, started_at: datetime
) -> str:
    dag_run_id = dag_run_id_for(DAG_ID, logical_date)
    conn.execute(
        text(
            """
            INSERT INTO catalog_dag_runs
                (dag_run_id, dag_id, logical_date, status, started_at)
            VALUES (:id, :dag, :d, 'INCOMPLETE', :t)
            ON CONFLICT (dag_run_id) DO UPDATE
                SET status = 'INCOMPLETE', started_at = EXCLUDED.started_at, finished_at = NULL
            """
        ),
        {"id": dag_run_id, "dag": DAG_ID, "d": logical_date, "t": started_at},
    )
    return dag_run_id
