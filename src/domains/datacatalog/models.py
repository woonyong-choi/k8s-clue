"""데이터 카탈로그 테이블 — 자산·계약 이력·리니지·품질 결과.

설계 근거는 docs/metadata-catalog.md 에 있다.

기존 domains/catalog 는 서비스 카탈로그이고 이 도메인과 무관하다.

핵심 두 가지.

1. 실행 단위를 dag_runs / collection_runs 2계층으로 나눈다.
   DAG 실행 하나에 collection_runs 행이 하나면, 소스가 넷일 때
   어느 소스가 실패했는지 기록할 자리가 없다. PARTIAL 은 여러 소스를
   가진 실행의 성질이므로 dag_runs 에만 존재한다.

2. 계약 이력을 asset_fields 와 분리한다.
   asset_fields 는 upsert 되므로 이전 세대 해시가 제자리에서 덮인다.
   append-only 인 schema_observations 가 없으면 "버전을 올리지 않은
   변경"을 판정할 기준점 자체가 사라진다.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from packages.contracts.catalog.vocabulary import (
    CHECK_SEVERITY,
    CHECK_TYPES,
    COLLECTION_RUN_STATUSES,
    DAG_RUN_STATUSES,
    HEALTHY_COLLECTION_STATUSES,
    QUALITY_SEVERITIES,
    QUALITY_STATUSES,
    UNHEALTHY_COLLECTION_STATUSES,
)
from packages.storage.base import Base, created_at_column, text_column

# 상태 어휘는 계약 모듈이 단일 정의입니다. 여기서 다시 적으면 MCP 도구 스키마·
# 조회 API 와 값이 어긋나고, 어긋난 뒤에는 어느 쪽이 맞는지 알 수 없습니다.
__all__ = [
    "CHECK_SEVERITY",
    "CHECK_TYPES",
    "COLLECTION_RUN_STATUSES",
    "DAG_RUN_STATUSES",
    "HEALTHY_COLLECTION_STATUSES",
    "UNHEALTHY_COLLECTION_STATUSES",
    "QUALITY_SEVERITIES",
    "QUALITY_STATUSES",
]


# 어휘는 계약 모듈이 단일 정의다. MCP 서버와 조회 API 도 같은 값을 읽는다.


def dag_run_id_for(dag_id: str, logical_date: str) -> str:
    """DAG 실행 식별자. attempt 를 넣지 않는다."""
    return f"{dag_id}__{logical_date}"


def collection_run_id_for(dag_run_id: str, source_id: str) -> str:
    """소스별 수집 식별자.

    attempt 를 넣지 않는 이유: Airflow 의 재시도는 task 단위라
    extract_loki 가 2회차이고 extract_kubernetes 가 1회차이면 두 task 가
    같은 DAG 실행 안에서 서로 다른 run_id 를 계산하게 된다.
    이후 모든 run_id 조인이 어긋난다.
    """
    return f"{dag_run_id}__{source_id}"


# 실행 -------------------------------------------------------------------


class CatalogDagRunRecord(Base):
    """DAG 실행 하나. PARTIAL 이 사는 곳."""

    __tablename__ = "catalog_dag_runs"
    __table_args__ = (Index("ix_catalog_dag_runs_scope", "logical_date", "status"),)

    dag_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    dag_id: Mapped[str] = text_column()
    logical_date: Mapped[Any] = mapped_column(Date, nullable=False)
    status: Mapped[str] = text_column()
    started_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    finished_at: Mapped[Any | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[Any] = created_at_column()


class CatalogCollectionRunRecord(Base):
    """소스별 수집 하나."""

    __tablename__ = "catalog_collection_runs"
    __table_args__ = (
        UniqueConstraint("dag_run_id", "source_id", name="uq_catalog_collection_runs_scope"),
        Index("ix_catalog_collection_runs_source", "source_id", "logical_date"),
    )

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    dag_run_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_dag_runs.dag_run_id", ondelete="CASCADE"), nullable=False
    )
    source_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_data_sources.source_id"), nullable=False
    )
    logical_date: Mapped[Any] = mapped_column(Date, nullable=False)
    status: Mapped[str] = text_column()
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    finished_at: Mapped[Any | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[Any] = created_at_column()


# 원천과 자산 -------------------------------------------------------------


class CatalogDataSourceRecord(Base):
    __tablename__ = "catalog_data_sources"

    source_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = text_column()
    source_type: Mapped[str] = text_column()
    owner: Mapped[str] = text_column()
    collection_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[Any] = created_at_column()


class CatalogDataAssetRecord(Base):
    """검색과 품질검사의 기본 단위.

    qualified_name 을 유일 키로 둔 이유는, 자산이 수백 개를 넘어 외부
    카탈로그 도구로 이관할 때 자산 식별자가 유지되어야 하기 때문이다.
    """

    __tablename__ = "catalog_data_assets"
    __table_args__ = (
        UniqueConstraint("qualified_name", name="uq_catalog_data_assets_qualified_name"),
    )

    asset_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_data_sources.source_id"), nullable=False
    )
    qualified_name: Mapped[str] = text_column()
    asset_type: Mapped[str] = text_column()
    freshness_sla_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    classification: Mapped[str] = text_column()
    owner: Mapped[str] = text_column()
    current_schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[Any] = created_at_column()


class CatalogAssetFieldRecord(Base):
    """현재 필드 계약. upsert 대상.

    유일 제약이 없으면 ON CONFLICT 가 실행되지 않는다. 제약 없이 insert 하면
    계약 행이 실행마다 복제되고, 필수 필드 위반 건수가 데이터가 아니라
    DAG 를 몇 번 돌렸는지의 함수가 된다.
    """

    __tablename__ = "catalog_asset_fields"
    __table_args__ = (
        UniqueConstraint(
            "asset_id", "schema_version", "field_path", name="uq_catalog_asset_fields"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_data_assets.asset_id", ondelete="CASCADE"), nullable=False
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    field_path: Mapped[str] = text_column()
    data_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    schema_hash: Mapped[str] = text_column()


class CatalogSchemaObservationRecord(Base):
    """계약 이력. append-only.

    같은 계약이 반복 관측되면 행이 늘지 않고, 계약이 바뀌면 행이 하나 생긴다.
    같은 (asset, version) 에 해시가 둘 이상이면 등록되지 않은 변경이 있었다는 뜻이다.
    """

    __tablename__ = "catalog_schema_observations"
    __table_args__ = (
        UniqueConstraint(
            "asset_id", "schema_version", "schema_hash", name="uq_catalog_schema_observations"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_data_assets.asset_id", ondelete="CASCADE"), nullable=False
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_hash: Mapped[str] = text_column()
    first_seen_run_id: Mapped[str] = text_column()
    first_seen_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


# 원본과 리니지 -----------------------------------------------------------


class CatalogRawSnapshotRecord(Base):
    """원본 보관 참조.

    content_hash 로 S3 객체는 중복 제거하되 행은 실행마다 남긴다.
    행까지 중복 제거하면 이틀 연속 같은 내용일 때 둘째 날의 역추적 경로가 끊긴다.
    """

    __tablename__ = "catalog_raw_snapshots"
    __table_args__ = (
        UniqueConstraint("run_id", "content_hash", name="uq_catalog_raw_snapshots"),
    )

    snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_collection_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    s3_uri: Mapped[str] = text_column()
    content_hash: Mapped[str] = text_column()
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[Any] = created_at_column()


class CatalogLineageEdgeRecord(Base):
    """upstream → downstream 관계.

    run_id 를 저장하고 조회할 때 collection_runs 에 조인한다.
    저장만 하고 조인하지 않으면 "언제 확인된 관계인가"에 답할 수 없다.
    """

    __tablename__ = "catalog_lineage_edges"
    __table_args__ = (
        UniqueConstraint(
            "upstream_asset_id",
            "downstream_asset_id",
            "run_id",
            name="uq_catalog_lineage_edges",
        ),
        Index("ix_catalog_lineage_downstream", "downstream_asset_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    upstream_asset_id: Mapped[str] = text_column()
    downstream_asset_id: Mapped[str] = text_column()
    transformation: Mapped[str] = text_column()
    run_id: Mapped[str] = text_column()


# 관측 데이터 -------------------------------------------------------------


class CatalogObservedRowRecord(Base):
    """이번 실행이 관측한 행. 최신성·필수 필드 검사 대상."""

    __tablename__ = "catalog_observed_rows"
    __table_args__ = (
        UniqueConstraint("run_id", "asset_id", "row_key", name="uq_catalog_observed_rows"),
        Index("ix_catalog_observed_rows_asset", "asset_id", "observed_at"),
    )

    row_id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[str] = text_column()
    asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_data_assets.asset_id", ondelete="CASCADE"), nullable=False
    )
    row_key: Mapped[str] = text_column()
    observed_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class CatalogObservedFieldRecord(Base):
    """관측된 필드와 타입. 드리프트 검사 대상.

    data_type 이 nullable 인 것은 의도적이다. 타입을 판별하지 못한 상태가
    가장 유력한 드리프트 신호이고, 그걸 표현할 수 있어야 한다.
    """

    __tablename__ = "catalog_observed_fields"
    __table_args__ = (
        UniqueConstraint("row_id", "field_path", name="uq_catalog_observed_fields"),
        Index("ix_catalog_observed_fields_run", "run_id", "asset_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    row_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_observed_rows.row_id", ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[str] = text_column()
    asset_id: Mapped[str] = text_column()
    field_path: Mapped[str] = text_column()
    data_type: Mapped[str | None] = mapped_column(Text, nullable=True)


class CatalogNormalizedEvidenceRecord(Base):
    """정규화된 수집 결과.

    유일 제약이 backfill 대응이다. 3일 재처리가 같은 관측을 다시 넣어도
    행이 늘지 않는다.
    """

    __tablename__ = "catalog_normalized_evidence"
    __table_args__ = (
        UniqueConstraint(
            "cluster_id",
            "source_id",
            "resource_uid",
            "observed_at",
            name="uq_catalog_normalized_evidence",
        ),
        Index("ix_catalog_normalized_evidence_lookup", "cluster_id", "observed_at"),
        # 08 중복 적재 검사용 커버링 인덱스. 그룹 키 3종 + run_id 로 정렬을 없애고,
        # INCLUDE 로 힙 접근을 없앤다. 없으면 같은 질의가 외부 정렬로 떨어진다.
        Index(
            "ix_catalog_normalized_evidence_dup",
            "cluster_id",
            "source_id",
            "resource_uid",
            "run_id",
            postgresql_include=("observed_at", "ingested_at"),
        ),
        Index("ix_catalog_normalized_evidence_asset", "asset_id", "observed_at"),
    )

    evidence_id: Mapped[str] = mapped_column(Text, primary_key=True)
    asset_id: Mapped[str] = text_column()
    run_id: Mapped[str] = text_column()
    cluster_id: Mapped[str] = text_column()
    source_id: Mapped[str] = text_column()
    resource_uid: Mapped[str] = text_column()
    collection_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    ingested_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


# 품질 결과 ---------------------------------------------------------------


class CatalogQualityResultRecord(Base):
    """검사 결과. 통과·실패 모두 적재한다.

    실패만 저장하면 "검사를 안 한 것"과 "검사했는데 통과한 것"을 구분할 수 없다.

    first_seen_dag_run_id 가 없으면 한 번 발생한 영구 위반이 이후 모든
    실행을 정합성 위반으로 만들어, 일주일이면 전부 붉어지고 신호가 잡음이 된다.
    """

    __tablename__ = "catalog_quality_results"
    __table_args__ = (
        # asset_id 에 NULL 대신 '-' 를 쓴다. NULL 은 유일 제약에서 서로
        # 다른 값으로 취급되어 ON CONFLICT 가 발화하지 않는다.
        # check_name(파일명)까지 키에 넣는 이유: 03·04 는 둘 다 SCHEMA_DRIFT
        # 지만 서로 다른 검사다. check_type 만으로는 구분되지 않는다.
        #
        # subject_key 가 키에 있어야 하는 이유: 한 검사가 한 자산에서 위반을
        # 여러 건 찾는다. 03 은 필드마다, 02 는 누락 필드마다, 08 은 리소스마다다.
        # 이 컬럼이 없으면 ON CONFLICT 가 같은 자산의 두 번째 위반부터 앞의 것을
        # 덮어써서, 자산당 1건만 남고 나머지는 조용히 사라진다. 통과·실패를 모두
        # 남긴다는 설계가 실패 쪽에서 깨진다.
        UniqueConstraint(
            "dag_run_id",
            "check_name",
            "asset_id",
            "subject_key",
            name="uq_catalog_quality_results",
        ),
        Index("ix_catalog_quality_results_open", "status", "severity"),
    )

    result_id: Mapped[str] = mapped_column(Text, primary_key=True)
    dag_run_id: Mapped[str] = mapped_column(
        Text, ForeignKey("catalog_dag_runs.dag_run_id", ondelete="CASCADE"), nullable=False
    )
    check_name: Mapped[str] = text_column()
    check_type: Mapped[str] = text_column()
    asset_id: Mapped[str] = mapped_column(Text, nullable=False, default="-")
    # 자산 안에서 위반을 구분하는 값. 필드 경로·리소스 UID 등이 들어간다.
    # 자산 단위 검사는 '-' 를 쓴다.
    subject_key: Mapped[str] = mapped_column(Text, nullable=False, default="-")
    status: Mapped[str] = text_column()
    severity: Mapped[str] = text_column()
    finding: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_dag_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    checked_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class CatalogLoadRecord(Base):
    """적재 멱등 키.

    run_id 를 키에 넣지 않는다. run_id 는 재시도마다 새로 생기므로
    키에 포함하면 재실행 때마다 행이 늘어난다.
    """

    __tablename__ = "catalog_loads"
    __table_args__ = (
        UniqueConstraint(
            "logical_date", "source_id", "asset_id", name="uq_catalog_load_idempotent"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    logical_date: Mapped[Any] = mapped_column(Date, nullable=False)
    source_id: Mapped[str] = text_column()
    asset_id: Mapped[str] = text_column()
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    loaded_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
