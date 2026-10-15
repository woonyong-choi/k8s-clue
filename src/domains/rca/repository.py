"""rca 도메인 repository — 증거·RCA 리포트 영속."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import Select, and_, case, cast, func, or_, select
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import insert as pg_insert

from domains.dashboard.models import RcaTimeline
from domains.rca.events import RecoveryVerificationFailedBody
from domains.rca.history import OBSERVATION_MAX_GAP_MS
from domains.rca.models import (
    Evidence,
    ImagePullObservation,
    IncidentSignalClaim,
    RcaBacklogItem,
    RcaReport,
    RecoveryPlanRecord,
)
from domains.rca.report_narrative import (
    RCA_NARRATIVE_PAYLOAD_KEY,
    RCA_NARRATIVE_STATUS_KEY,
)
from domains.rca.report_projection import rca_report_projection
from packages.contracts.event_bus.interfaces import JsonObject
from packages.contracts.event_bus.processing import EventProcessingStatus
from packages.events.envelope import event
from packages.storage.engine import DatabaseConnection, iso_or_none
from packages.storage.schema import EventModel, EventProcessing

RECOVERY_PLAN_STATUS_SELECTION_REQUESTED = "selection_requested"
RECOVERY_PLAN_STATUS_SELECTED = "selected"
RECOVERY_PLAN_STATUS_PR_OPEN = "pr_open"
RECOVERY_PLAN_STATUS_DEPLOY_PENDING = "deploy_pending"
RECOVERY_PLAN_STATUS_VERIFICATION_PENDING = "verification_pending"
RECOVERY_PLAN_STATUS_COMPLETED = "completed"
RECOVERY_PLAN_STATUS_FAILED = "failed"
RECOVERY_PLAN_MONOTONIC_STATUSES = (
    RECOVERY_PLAN_STATUS_SELECTED,
    RECOVERY_PLAN_STATUS_PR_OPEN,
    RECOVERY_PLAN_STATUS_DEPLOY_PENDING,
    RECOVERY_PLAN_STATUS_VERIFICATION_PENDING,
    RECOVERY_PLAN_STATUS_COMPLETED,
    RECOVERY_PLAN_STATUS_FAILED,
)
BACKLOG_STATUS_OPEN = "open"
BACKLOG_STATUS_RESOLVED = "resolved"
BACKLOG_RULE_RESOLVED_REASON = "matching RCA rule is now available"
OPEN_RECOVERY_PLAN_STATUSES = (RECOVERY_PLAN_STATUS_SELECTION_REQUESTED,)
RCA_REPORT_REPOSITORY_OWNED_COLUMNS = frozenset(
    {
        "id",
        "workspace_id",
        "correlation_id",
        "root_cause",
        "action",
        "payload",
        "created_at",
    }
)
RCA_REPORT_STORAGE_PROJECTION_COLUMNS = frozenset(RcaReport.__table__.c.keys()).difference(
    RCA_REPORT_REPOSITORY_OWNED_COLUMNS
)
ALERTMANAGER_EVIDENCE_ACTIVE = "active"
ALERTMANAGER_EVIDENCE_PENDING = "pending"
ALERTMANAGER_EVIDENCE_TERMINAL = "terminal"
ALERTMANAGER_EVIDENCE_ORPHAN = "orphan"
ALERTMANAGER_TERMINAL_TIMELINE_STATUSES = frozenset(
    {
        "incident_resolved",
        "incident_expired",
    }
)
ALERTMANAGER_IN_FLIGHT_PROCESSING_STATUSES = (
    EventProcessingStatus.PROCESSING,
    EventProcessingStatus.RETRYING,
)


def rca_report_storage_projection(body: JsonObject) -> JsonObject:
    """Filter report projection through the actual storage schema.

    New response-only fields can be added to ``rca_report_projection`` without
    becoming unexpected INSERT kwargs. A field reaches dedicated storage only
    after the model/migration explicitly adds its column.
    """
    projection = rca_report_projection(body)
    return {
        field: value
        for field, value in projection.items()
        if field in RCA_REPORT_STORAGE_PROJECTION_COLUMNS
    }


def _rca_report_summary_columns() -> tuple[Any, ...]:
    """목록 API 에 필요한 작은 projection 만 읽어 payload DB I/O 를 피한다."""
    table = RcaReport.__table__
    return (
        table.c.id,
        table.c.workspace_id,
        table.c.correlation_id,
        table.c.root_cause,
        table.c.action,
        table.c.incident_id,
        table.c.cluster_id,
        table.c.symptom,
        table.c.severity,
        table.c.payload["incident"]["first_seen_at"].astext.label("first_seen_at"),
        table.c.confidence,
        table.c.reason,
        table.c.evidence_ref,
        table.c.supporting_evidence,
        table.c.missing_evidence,
        table.c.payload["rca_detail"]["evidence_summary"].astext.label("evidence_summary"),
        table.c.payload["rca_detail"]["evidence_bundle_summary"].astext.label(
            "evidence_bundle_summary"
        ),
        table.c.resource_kind,
        table.c.resource_name,
        table.c.namespace,
        table.c.secondary_symptoms,
        table.c.selected_candidate_id,
        table.c.candidates,
        table.c.supporting_evidence_refs,
        table.c.missing_evidence_checks,
        table.c.payload[RCA_NARRATIVE_PAYLOAD_KEY].label(RCA_NARRATIVE_PAYLOAD_KEY),
        table.c.payload[RCA_NARRATIVE_STATUS_KEY].astext.label(RCA_NARRATIVE_STATUS_KEY),
        table.c.created_at,
    )


class RcaRepository(DatabaseConnection):
    def current_database_time(self) -> datetime:
        with self.connection() as conn:
            value = conn.execute(select(func.now())).scalar_one()
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    def get_alertmanager_evidence_disposition(
        self,
        workspace_id: str,
        correlation_id: str,
        event_id: str,
    ) -> str:
        """Classify whether an Alertmanager evidence key can still deduplicate.

        Evidence windows are durable idempotency records, not the incident
        lifecycle itself. A manually removed incident must not leave a window
        permanently pointing at a correlation that no longer has an origin
        event or any incident projection.
        """

        timeline = RcaTimeline.__table__
        reports = RcaReport.__table__
        plans = RecoveryPlanRecord.__table__
        claims = IncidentSignalClaim.__table__
        events = EventModel.__table__
        processing = EventProcessing.__table__
        with self.connection() as conn:
            latest_status = conn.execute(
                select(timeline.c.status)
                .where(
                    timeline.c.workspace_id == workspace_id,
                    or_(
                        timeline.c.correlation_id == correlation_id,
                        timeline.c.incident_id == correlation_id,
                    ),
                )
                .order_by(timeline.c.updated_at.desc(), timeline.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_status is not None:
                return (
                    ALERTMANAGER_EVIDENCE_TERMINAL
                    if str(latest_status) in ALERTMANAGER_TERMINAL_TIMELINE_STATUSES
                    else ALERTMANAGER_EVIDENCE_ACTIVE
                )

            latest_plan_status = conn.execute(
                select(plans.c.status)
                .where(
                    plans.c.workspace_id == workspace_id,
                    or_(
                        plans.c.correlation_id == correlation_id,
                        plans.c.incident_id == correlation_id,
                    ),
                )
                .order_by(plans.c.updated_at.desc(), plans.c.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_plan_status is not None:
                return (
                    ALERTMANAGER_EVIDENCE_TERMINAL
                    if str(latest_plan_status) == RECOVERY_PLAN_STATUS_COMPLETED
                    else ALERTMANAGER_EVIDENCE_ACTIVE
                )

            report_exists = conn.execute(
                select(reports.c.id)
                .where(
                    reports.c.workspace_id == workspace_id,
                    or_(
                        reports.c.correlation_id == correlation_id,
                        reports.c.incident_id == correlation_id,
                    ),
                )
                .limit(1)
            ).scalar_one_or_none()
            if report_exists is not None:
                return ALERTMANAGER_EVIDENCE_ACTIVE

            claim_exists = conn.execute(
                select(claims.c.id)
                .where(
                    claims.c.workspace_id == workspace_id,
                    claims.c.first_correlation_id == correlation_id,
                )
                .limit(1)
            ).scalar_one_or_none()
            if claim_exists is not None:
                return ALERTMANAGER_EVIDENCE_ACTIVE

            origin_exists = conn.execute(
                select(events.c.event_id)
                .where(
                    events.c.event_id == event_id,
                    events.c.correlation_id == correlation_id,
                )
                .limit(1)
            ).scalar_one_or_none()
            if origin_exists is None:
                return ALERTMANAGER_EVIDENCE_ORPHAN

            in_flight_exists = conn.execute(
                select(processing.c.event_id)
                .where(
                    processing.c.event_id == event_id,
                    processing.c.status.in_(ALERTMANAGER_IN_FLIGHT_PROCESSING_STATUSES),
                )
                .limit(1)
            ).scalar_one_or_none()
            if in_flight_exists is not None:
                return ALERTMANAGER_EVIDENCE_PENDING

            processed_exists = conn.execute(
                select(processing.c.event_id)
                .where(processing.c.event_id == event_id)
                .limit(1)
            ).scalar_one_or_none()
            return (
                ALERTMANAGER_EVIDENCE_ORPHAN
                if processed_exists is not None
                else ALERTMANAGER_EVIDENCE_PENDING
            )

    def save_evidence(
        self, correlation_id: str, workspace_id: str, kind: str, body: JsonObject
    ) -> None:
        table = Evidence.__table__
        statement = pg_insert(table).values(
            workspace_id=workspace_id,
            correlation_id=correlation_id,
            kind=kind,
            payload=body,
        )
        with self.connection() as conn:
            conn.execute(statement)

    def get_evidence_payload(
        self,
        workspace_id: str,
        correlation_id: str,
        kind: str,
    ) -> JsonObject | None:
        table = Evidence.__table__
        statement = (
            select(table.c.payload)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.correlation_id == correlation_id,
                table.c.kind == kind,
            )
            .order_by(table.c.created_at.desc(), table.c.id.desc())
            .limit(1)
        )
        with self.connection() as conn:
            payload = conn.execute(statement).scalar_one_or_none()
        return payload if isinstance(payload, dict) else None

    def claim_incident_signal(
        self,
        workspace_id: str,
        cluster_id: str,
        signal_key: str,
        correlation_id: str,
        payload: JsonObject,
    ) -> bool:
        """Atomically claim one concrete termination before emitting an incident.

        PostgreSQL arbitrates concurrent workers through the unique identity.
        Returning ``False`` means this exact termination was already handled,
        including by a worker process that has since restarted.
        """
        table = IncidentSignalClaim.__table__
        statement = (
            pg_insert(table)
            .values(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                signal_key=signal_key,
                first_correlation_id=correlation_id,
                payload=payload,
            )
            .on_conflict_do_nothing(
                constraint="uq_incident_signal_claim_identity",
            )
            .returning(table.c.id)
        )
        with self.connection() as conn:
            return conn.execute(statement).scalar_one_or_none() is not None

    def upsert_rca_backlog_item(self, body: JsonObject) -> None:
        table = RcaBacklogItem.__table__
        insert_statement = pg_insert(table).values(
            backlog_id=body["backlog_id"],
            workspace_id=body["workspace_id"],
            incident_id=body["incident_id"],
            symptom=body["symptom"],
            title=body["title"],
            reason=body["reason"],
            evidence_ref=body["evidence_ref"],
            missing_evidence={"items": body["missing_evidence"]},
            status=body["status"],
            occurrence_count=1,
            payload=body["payload"],
            updated_at=func.now(),
        )
        statement = insert_statement.on_conflict_do_update(
            index_elements=[table.c.backlog_id],
            set_={
                "incident_id": insert_statement.excluded.incident_id,
                "reason": insert_statement.excluded.reason,
                "evidence_ref": insert_statement.excluded.evidence_ref,
                "missing_evidence": insert_statement.excluded.missing_evidence,
                "status": insert_statement.excluded.status,
                "occurrence_count": table.c.occurrence_count + 1,
                "payload": insert_statement.excluded.payload,
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)

    def resolve_rca_backlog_item_for_rule(
        self,
        workspace_id: str,
        symptom: str,
        reason: str = BACKLOG_RULE_RESOLVED_REASON,
    ) -> int:
        """매칭 룰이 생긴 symptom 의 missing-rule backlog 를 닫는다."""
        table = RcaBacklogItem.__table__
        backlog_id = f"missing-cause-rule:{workspace_id}:{symptom}"
        statement = (
            table.update()
            .where(
                table.c.backlog_id == backlog_id,
                table.c.workspace_id == workspace_id,
                table.c.status == BACKLOG_STATUS_OPEN,
            )
            .values(status=BACKLOG_STATUS_RESOLVED, reason=reason, updated_at=func.now())
            .returning(table.c.backlog_id)
        )
        with self.connection() as conn:
            return len(conn.execute(statement).all())

    def list_rca_reports(self, workspace_id: str, *, limit: int = 5) -> list[JsonObject]:
        """최근 RCA 리포트 조회(최신순) — AI 도구 등 읽기 전용 소비자용."""
        table = RcaReport.__table__
        statement = (
            select(table)
            .where(table.c.workspace_id == workspace_id)
            .order_by(table.c.created_at.desc(), table.c.id.desc())
            .limit(limit)
        )
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]

    def list_evidence_records(
        self,
        workspace_id: str,
        *,
        correlation_id: str | None = None,
        kind: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[datetime, int] | None = None,
    ) -> list[JsonObject]:
        """워크스페이스 범위 evidence 목록(최신순) — /evidence 범용 조회 API 용.

        since 는 포함(>=), until 은 미포함(<) 경계.
        cursor 가 있으면 (created_at, id) keyset 을 우선하고, 없으면 offset 하위호환을 쓴다.
        """
        table = Evidence.__table__
        statement: Select[Any] = (
            select(table)
            .where(table.c.workspace_id == workspace_id)
            .order_by(table.c.created_at.desc(), table.c.id.desc())
            .limit(limit)
        )
        if correlation_id is not None:
            statement = statement.where(table.c.correlation_id == correlation_id)
        if kind is not None:
            statement = statement.where(table.c.kind == kind)
        statement = _apply_created_at_window(statement, table, since, until)
        statement = _apply_keyset_or_offset(statement, table, cursor, offset)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [_serialize_created_at(row) for row in rows]

    def list_rca_report_records(
        self,
        workspace_id: str,
        *,
        correlation_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[datetime, int] | None = None,
    ) -> list[JsonObject]:
        """워크스페이스 범위 RCA report 목록(최신순) — /rca-reports 조회 API 용.

        since 는 포함(>=), until 은 미포함(<) 경계. payload 요약은 라우터가 수행함.
        cursor 가 있으면 (created_at, id) keyset 을 우선하고, 없으면 offset 하위호환을 쓴다.
        """
        table = RcaReport.__table__
        statement: Select[Any] = (
            select(*_rca_report_summary_columns())
            .where(table.c.workspace_id == workspace_id)
            .order_by(table.c.created_at.desc(), table.c.id.desc())
            .limit(limit)
        )
        if correlation_id is not None:
            statement = statement.where(table.c.correlation_id == correlation_id)
        statement = _apply_created_at_window(statement, table, since, until)
        statement = _apply_keyset_or_offset(statement, table, cursor, offset)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [_serialize_created_at(row) for row in rows]

    def upsert_recovery_selection_request(
        self,
        correlation_id: str,
        workspace_id: str,
        plan: JsonObject,
    ) -> None:
        self.upsert_recovery_plan(
            correlation_id,
            workspace_id,
            plan,
            status=RECOVERY_PLAN_STATUS_SELECTION_REQUESTED,
        )

    def upsert_recovery_plan(
        self,
        correlation_id: str,
        workspace_id: str,
        plan: JsonObject,
        *,
        status: str,
        selected_action_id: str | None = None,
        selected_by: str | None = None,
    ) -> None:
        """Persist every generated plan, including the auto-selected path.

        recovery.planned is an operator-visible read model boundary. Previously only
        selection_requested plans were stored, so a valid recovery.planned audit event
        could lead to a 404 in the incident detail after automatic selection.
        """
        if status not in {
            RECOVERY_PLAN_STATUS_SELECTION_REQUESTED,
            RECOVERY_PLAN_STATUS_SELECTED,
        }:
            raise ValueError(f"Unsupported recovery plan status: {status}")
        table = RecoveryPlanRecord.__table__
        insert = pg_insert(table).values(
            plan_id=str(plan["plan_id"]),
            workspace_id=workspace_id,
            correlation_id=correlation_id,
            incident_id=str(plan["incident_id"]),
            evidence_ref=str(plan["evidence_ref"]),
            status=status,
            selected_action_id=selected_action_id,
            selected_by=selected_by,
            payload=plan,
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.workspace_id, table.c.plan_id],
            set_={
                "correlation_id": insert.excluded.correlation_id,
                "incident_id": insert.excluded.incident_id,
                "evidence_ref": insert.excluded.evidence_ref,
                "status": case(
                    (
                        table.c.status.in_(RECOVERY_PLAN_MONOTONIC_STATUSES),
                        table.c.status,
                    ),
                    else_=insert.excluded.status,
                ),
                "selected_action_id": case(
                    (
                        table.c.status.in_(RECOVERY_PLAN_MONOTONIC_STATUSES),
                        table.c.selected_action_id,
                    ),
                    else_=insert.excluded.selected_action_id,
                ),
                "selected_by": case(
                    (
                        table.c.status.in_(RECOVERY_PLAN_MONOTONIC_STATUSES),
                        table.c.selected_by,
                    ),
                    else_=insert.excluded.selected_by,
                ),
                "payload": case(
                    (
                        table.c.status.in_(RECOVERY_PLAN_MONOTONIC_STATUSES),
                        table.c.payload,
                    ),
                    else_=insert.excluded.payload,
                ),
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)

    def get_recovery_plan(self, plan_id: str, workspace_id: str) -> JsonObject | None:
        table = RecoveryPlanRecord.__table__
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(table.c.plan_id == plan_id, table.c.workspace_id == workspace_id)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return dict(row) if row else None

    def get_recovery_plan_by_correlation(
        self, correlation_id: str, workspace_id: str
    ) -> JsonObject | None:
        table = RecoveryPlanRecord.__table__
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(
                table.c.correlation_id == correlation_id,
                table.c.workspace_id == workspace_id,
            )
            .order_by(table.c.updated_at.desc(), table.c.id.desc())
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return dict(row) if row else None

    # cost: time O(p), heap O(p), stack O(1), io 1
    # vars: p = 최대 100개 활성 복구 계획
    # basis: estimate
    def list_image_pull_tracking_plans(
        self, workspace_id: str, cluster_id: str, *, limit: int = 100
    ) -> list[JsonObject]:
        table = RecoveryPlanRecord.__table__
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.status,
                table.c.payload,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.status.in_(
                    (RECOVERY_PLAN_STATUS_DEPLOY_PENDING, RECOVERY_PLAN_STATUS_VERIFICATION_PENDING)
                ),
                table.c.payload["target"]["cluster_id"].astext == cluster_id,
            )
            .order_by(table.c.updated_at.asc(), table.c.id.asc())
            .limit(max(1, min(limit, 100)))
        )
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]

    # cost: time O(log n), heap O(1), stack O(1), io 1
    # vars: n = 한 사건의 이미지 증거 창 수
    # basis: estimate
    def record_image_pull_observation(self, payload: JsonObject) -> JsonObject:
        table = ImagePullObservation.__table__
        values = {
            "workspace_id": str(payload["workspace_id"]),
            "correlation_id": str(payload["correlation_id"]),
            "binding_id": str(payload["binding_id"]),
            "expected_sha": str(payload["expected_sha"]),
            "expected_image": str(payload["expected_image"]),
            "evidence_key": str(payload["evidence_key"]),
            "status": str(payload["status"]),
            "reason": str(payload["reason"]),
            "deployment_uid": payload.get("deployment_uid"),
            "replicaset_uid": payload.get("replicaset_uid"),
            "ready_pod_count": int(payload.get("ready_pod_count") or 0),
            "observed_at": payload["observed_at"],
        }
        identity = "\0".join(
            str(values[key])
            for key in ("workspace_id", "correlation_id", "binding_id", "expected_sha")
        )
        lock_key = int.from_bytes(
            hashlib.sha256(f"image-pull\0{identity}".encode()).digest()[:8],
            byteorder="big",
            signed=True,
        )
        with self.connection() as conn:
            conn.execute(select(func.pg_advisory_xact_lock(lock_key)))
            existing = (
                conn.execute(
                    select(table).where(
                        table.c.workspace_id == values["workspace_id"],
                        table.c.correlation_id == values["correlation_id"],
                        table.c.binding_id == values["binding_id"],
                        table.c.expected_sha == values["expected_sha"],
                        table.c.evidence_key == values["evidence_key"],
                    )
                )
                .mappings()
                .first()
            )
            if existing is not None:
                return dict(existing)
            previous = (
                conn.execute(
                    select(table)
                    .where(
                        table.c.workspace_id == values["workspace_id"],
                        table.c.correlation_id == values["correlation_id"],
                        table.c.binding_id == values["binding_id"],
                        table.c.expected_sha == values["expected_sha"],
                    )
                    .order_by(table.c.observed_at.desc(), table.c.id.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            is_late = previous is not None and values["observed_at"] <= previous["observed_at"]
            gap_ms = (
                (values["observed_at"] - previous["observed_at"]).total_seconds() * 1000
                if previous is not None
                else None
            )
            follows_healthy = (
                previous is not None
                and previous["status"] == "healthy"
                and previous["expected_image"] == values["expected_image"]
                and previous["deployment_uid"] == values["deployment_uid"]
                and previous["replicaset_uid"] == values["replicaset_uid"]
                and gap_ms is not None
                and 0 < gap_ms <= OBSERVATION_MAX_GAP_MS
            )
            if is_late:
                values["status"] = "unknown"
                values["reason"] = "late_evidence_window"
            consecutive = (
                int(previous["consecutive_healthy_windows"]) + 1
                if values["status"] == "healthy" and follows_healthy
                else 1
                if values["status"] == "healthy"
                else 0
            )
            values["consecutive_healthy_windows"] = consecutive
            values["confirmed_recovered"] = consecutive >= 2
            row = conn.execute(pg_insert(table).values(**values).returning(table)).mappings().one()
        return dict(row)

    # cost: time O(log n + i), heap O(i), stack O(1), io 1
    # vars: n = 이미지 증거 전체 행 수, i = 제한된 반환 행 수
    # basis: estimate
    def list_image_pull_observations(
        self, workspace_id: str, correlation_id: str, binding_id: str, *, limit: int = 100
    ) -> list[JsonObject]:
        table = ImagePullObservation.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.correlation_id == correlation_id,
                table.c.binding_id == binding_id,
            )
            .order_by(table.c.observed_at.desc(), table.c.id.desc())
            .limit(max(1, min(limit, 100)))
        )
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]

    def select_recovery_plan_action_if_open(
        self,
        plan_id: str,
        workspace_id: str,
        action_id: str,
        selected_by: str,
    ) -> JsonObject | None:
        table = RecoveryPlanRecord.__table__
        with self.unit_of_work() as conn:
            current = (
                conn.execute(
                    select(table)
                    .where(
                        table.c.plan_id == plan_id,
                        table.c.workspace_id == workspace_id,
                        table.c.status.in_(OPEN_RECOVERY_PLAN_STATUSES),
                    )
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if current is None:
                return None
            payload = deepcopy(dict(current["payload"]))
            previous_lifecycle = (
                payload.get("lifecycle")
                if isinstance(payload.get("lifecycle"), Mapping)
                else {}
            )
            previous_attempt = (
                previous_lifecycle.get("attempt")
                if isinstance(previous_lifecycle.get("attempt"), Mapping)
                else {}
            )
            try:
                attempt_number = max(0, int(previous_attempt.get("number") or 0)) + 1
            except (TypeError, ValueError):
                attempt_number = 1
            payload["lifecycle"] = {
                "phase": RECOVERY_PLAN_STATUS_SELECTED,
                "attempt": {
                    "id": f"recovery-attempt-{uuid4()}",
                    "number": attempt_number,
                    "action_id": action_id,
                    "selected_by": selected_by,
                    "selected_at": datetime.now(UTC).isoformat(),
                },
            }
            row = (
                conn.execute(
                    table.update()
                    .where(
                        table.c.id == current["id"],
                        table.c.status.in_(OPEN_RECOVERY_PLAN_STATUSES),
                    )
                    .values(
                        status=RECOVERY_PLAN_STATUS_SELECTED,
                        selected_action_id=action_id,
                        selected_by=selected_by,
                        payload=payload,
                        updated_at=func.now(),
                    )
                    .returning(table.c.payload, table.c.correlation_id)
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def reopen_recovery_plan_action(
        self,
        plan_id: str,
        workspace_id: str,
        action_id: str,
    ) -> bool:
        """Retryable dispatch failure 뒤 같은 선택만 원자적으로 다시 연다."""

        table = RecoveryPlanRecord.__table__
        statement = (
            table.update()
            .where(
                table.c.plan_id == plan_id,
                table.c.workspace_id == workspace_id,
                table.c.status == RECOVERY_PLAN_STATUS_SELECTED,
                table.c.selected_action_id == action_id,
            )
            .values(
                status=RECOVERY_PLAN_STATUS_SELECTION_REQUESTED,
                selected_action_id=None,
                selected_by=None,
                updated_at=func.now(),
            )
            .returning(table.c.plan_id)
        )
        with self.connection() as conn:
            return conn.execute(statement).scalar_one_or_none() is not None

    def update_recovery_plan_lifecycle_if_status(
        self,
        plan_id: str,
        workspace_id: str,
        *,
        expected_statuses: tuple[str, ...],
        status: str,
        lifecycle: JsonObject,
        clear_selection: bool = False,
    ) -> JsonObject | None:
        """CAS recovery lifecycle transition while preserving the immutable plan.

        The lifecycle is additive JSON inside the existing payload.  The status
        predicate prevents a stale webhook/workflow/evidence replay from moving a
        later recovery attempt backwards.
        """

        table = RecoveryPlanRecord.__table__
        with self.unit_of_work() as conn:
            current = (
                conn.execute(
                    select(table)
                    .where(
                        table.c.plan_id == plan_id,
                        table.c.workspace_id == workspace_id,
                        table.c.status.in_(expected_statuses),
                    )
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if current is None:
                return None
            payload = deepcopy(dict(current["payload"]))
            payload["lifecycle"] = deepcopy(lifecycle)
            update_values: dict[str, Any] = {
                "status": status,
                "payload": payload,
                "updated_at": func.now(),
            }
            if clear_selection:
                update_values.update(
                    {
                        "selected_action_id": None,
                        "selected_by": None,
                    }
                )
            row = (
                conn.execute(
                    table.update()
                    .where(
                        table.c.id == current["id"],
                        table.c.status.in_(expected_statuses),
                    )
                    .values(**update_values)
                    .returning(
                        table.c.plan_id,
                        table.c.workspace_id,
                        table.c.correlation_id,
                        table.c.incident_id,
                        table.c.evidence_ref,
                        table.c.status,
                        table.c.selected_action_id,
                        table.c.selected_by,
                        table.c.payload,
                    )
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def find_open_recovery_plan_for_pull_request(
        self,
        *,
        pr_url: str,
        repo_ref: str,
        base_branch: str,
        pr_number: int,
        pr_node_id: str,
        head_ref: str,
        head_sha: str,
    ) -> JsonObject | None:
        """Match one signed merge webhook to one exact tracked Safe PR.

        No workspace is accepted from the webhook.  Tenant scope comes only from
        the unique stored row, and ambiguous matches fail closed.
        """

        table = RecoveryPlanRecord.__table__
        lifecycle = table.c.payload["lifecycle"]
        pr = lifecycle["pr"]
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(
                table.c.status == RECOVERY_PLAN_STATUS_PR_OPEN,
                pr["url"].astext == pr_url,
                func.lower(pr["repo_ref"].astext) == repo_ref.casefold(),
                pr["base_branch"].astext == base_branch,
                pr["number"].astext == str(pr_number),
                pr["node_id"].astext == pr_node_id,
                pr["head_ref"].astext == head_ref,
                pr["head_sha"].astext == head_sha,
            )
            .limit(2)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return dict(rows[0]) if len(rows) == 1 else None

    def find_open_recovery_plan_for_pull_request_base_identity(
        self,
        *,
        pr_url: str,
        repo_ref: str,
        base_branch: str,
        pr_number: int,
        pr_node_id: str,
        head_ref: str,
    ) -> JsonObject | None:
        """Resolve a tracked PR without head SHA to detect force-push/synchronize."""

        table = RecoveryPlanRecord.__table__
        pr = table.c.payload["lifecycle"]["pr"]
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(
                table.c.status == RECOVERY_PLAN_STATUS_PR_OPEN,
                pr["url"].astext == pr_url,
                func.lower(pr["repo_ref"].astext) == repo_ref.casefold(),
                pr["base_branch"].astext == base_branch,
                pr["number"].astext == str(pr_number),
                pr["node_id"].astext == pr_node_id,
                pr["head_ref"].astext == head_ref,
            )
            .limit(2)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return dict(rows[0]) if len(rows) == 1 else None

    def get_recovery_plan_for_workflow(
        self,
        workspace_id: str,
        workflow_run_id: str,
        binding_id: str,
        application_id: str,
    ) -> JsonObject | None:
        """Resolve only the merge-derived exact binding deployment."""

        table = RecoveryPlanRecord.__table__
        merge = table.c.payload["lifecycle"]["merge"]
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.status.in_(
                    (
                        RECOVERY_PLAN_STATUS_DEPLOY_PENDING,
                        RECOVERY_PLAN_STATUS_VERIFICATION_PENDING,
                        RECOVERY_PLAN_STATUS_COMPLETED,
                        RECOVERY_PLAN_STATUS_FAILED,
                    )
                ),
                merge["workflow_run_id"].astext == workflow_run_id,
                merge["binding_id"].astext == binding_id,
                merge["application_id"].astext == application_id,
            )
            .limit(2)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return dict(rows[0]) if len(rows) == 1 else None

    def list_recovery_verification_plans(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        limit: int = 100,
    ) -> list[JsonObject]:
        table = RecoveryPlanRecord.__table__
        target = table.c.payload["target"]
        statement = (
            select(
                table.c.plan_id,
                table.c.workspace_id,
                table.c.correlation_id,
                table.c.incident_id,
                table.c.evidence_ref,
                table.c.status,
                table.c.selected_action_id,
                table.c.selected_by,
                table.c.payload,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.status == RECOVERY_PLAN_STATUS_VERIFICATION_PENDING,
                target["cluster_id"].astext == cluster_id,
            )
            .order_by(table.c.updated_at.asc(), table.c.id.asc())
            .limit(max(1, min(limit, 500)))
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def expire_recovery_verifications(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> list[JsonObject]:
        """Fail overdue verification plans even when evidence delivery stops.

        The state transition and failure event outbox entry share one database
        transaction, so a janitor crash cannot leave a silently failed plan.
        ``now`` is injectable for boundary tests; production uses DB time.
        """

        table = RecoveryPlanRecord.__table__
        bounded_limit = max(1, min(int(limit), 500))
        expired: list[JsonObject] = []
        with self.unit_of_work() as conn:
            effective_now = now or conn.execute(select(func.now())).scalar_one()
            if effective_now.tzinfo is None:
                effective_now = effective_now.replace(tzinfo=UTC)
            deadline = cast(
                table.c.payload["lifecycle"]["verification"]["deadline_at"].astext,
                TIMESTAMP(timezone=True),
            )
            rows = (
                conn.execute(
                    select(table)
                    .where(
                        table.c.status == RECOVERY_PLAN_STATUS_VERIFICATION_PENDING,
                        deadline <= effective_now,
                    )
                    .order_by(deadline.asc(), table.c.id.asc())
                    .limit(bounded_limit)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .all()
            )
            staged = []
            for row in rows:
                payload = deepcopy(dict(row["payload"]))
                lifecycle = dict(payload.get("lifecycle") or {})
                verification = dict(lifecycle.get("verification") or {})
                reason = (
                    "최대 검증 시간 안에 연속 정상화 근거가 충족되지 않아 "
                    "복구 완료로 판정하지 않았습니다."
                )
                verification.update(
                    {
                        "status": RECOVERY_PLAN_STATUS_FAILED,
                        "last_reason_code": "verification_window_expired",
                        "last_reason": reason,
                        "expired_at": effective_now.isoformat(),
                    }
                )
                lifecycle.update(
                    {
                        "phase": RECOVERY_PLAN_STATUS_FAILED,
                        "verification": verification,
                        "failure": {
                            "reason_code": "verification_window_expired",
                            "reason": reason,
                        },
                    }
                )
                payload["lifecycle"] = lifecycle
                updated = (
                    conn.execute(
                        table.update()
                        .where(
                            table.c.id == row["id"],
                            table.c.status
                            == RECOVERY_PLAN_STATUS_VERIFICATION_PENDING,
                        )
                        .values(
                            status=RECOVERY_PLAN_STATUS_FAILED,
                            payload=payload,
                            updated_at=func.now(),
                        )
                        .returning(
                            table.c.plan_id,
                            table.c.workspace_id,
                            table.c.correlation_id,
                            table.c.incident_id,
                            table.c.evidence_ref,
                            table.c.status,
                            table.c.payload,
                        )
                    )
                    .mappings()
                    .first()
                )
                if updated is None:
                    continue
                body = RecoveryVerificationFailedBody(
                    plan_id=str(updated["plan_id"]),
                    incident_id=str(updated["incident_id"]),
                    reason_code="verification_window_expired",
                    reason=reason,
                    evidence_ref=str(updated["evidence_ref"]),
                    before=dict(verification.get("before") or {}),
                    after=dict(verification.get("after") or {}),
                    workspace_id=str(updated["workspace_id"]),
                )
                staged.append(
                    event(
                        body.__subject__.value,
                        "rca-timeline-janitor",
                        body.to_body(),
                        correlation_id=str(updated["correlation_id"]),
                        workspace_id=str(updated["workspace_id"]),
                    )
                )
                expired.append(dict(updated))
            if staged:
                record_event = getattr(self, "record_event", None)
                stage_events = getattr(self, "stage_events", None)
                if not callable(record_event) or not callable(stage_events):
                    raise RuntimeError("recovery verification outbox support unavailable")
                for envelope in staged:
                    record_event(envelope)
                stage_events(conn, staged)
        return expired

    def save_rca_report(
        self,
        correlation_id: str,
        workspace_id: str,
        root_cause: str,
        action: str,
        body: JsonObject,
    ) -> None:
        table = RcaReport.__table__
        projection = rca_report_storage_projection(body)
        # Narrative and additive evidence summaries remain in the existing
        # JSON payload. The same is true for first_seen_at; the report list reads
        # it from payload and ``rca_reports`` intentionally has no such column.
        statement = pg_insert(table).values(
            workspace_id=workspace_id,
            correlation_id=correlation_id,
            root_cause=root_cause,
            action=action,
            **projection,
            payload=body,
        )
        with self.connection() as conn:
            conn.execute(statement)

    def find_recent_rca_report(
        self,
        workspace_id: str,
        root_cause: str,
        resource_key: str,
        window_seconds: int,
    ) -> JsonObject | None:
        """같은 (workspace, root_cause, 대상 리소스) 리포트가 최근 window 안에 있는지 조회.

        장애가 지속되는 동안 evidence 주기(~10s)마다 동일 리포트가 무한 적재되는 것을
        막는 dedup 조회 — rca-worker 가 저장 전에 호출한다(있으면 저장 생략).
        리소스 비교는 저장 시 만든 projection 컬럼을 Python 에서
        `rca_report_resource_key` 로 비교한다(JSONB 원문 읽기 방지).
        """
        table = RcaReport.__table__
        threshold = datetime.now(UTC) - timedelta(seconds=window_seconds)
        statement = (
            select(
                table.c.id,
                table.c.correlation_id,
                table.c.cluster_id,
                table.c.namespace,
                table.c.resource_kind,
                table.c.resource_name,
                table.c.created_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.root_cause == root_cause,
                table.c.created_at >= threshold,
            )
            .order_by(table.c.created_at.desc(), table.c.id.desc())
            .limit(20)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        for row in rows:
            if rca_report_resource_key(row) == resource_key:
                return {
                    "id": row["id"],
                    "correlation_id": row["correlation_id"],
                    "created_at": iso_or_none(row.get("created_at")),
                }
        return None


def rca_report_resource_key(incident: Mapping[str, Any] | None) -> str:
    """리포트 dedup 용 대상 리소스 키 — `namespace/kind/name` (incident 없으면 "unknown")."""
    if not isinstance(incident, Mapping):
        return "unknown"
    return (
        f"{incident.get('namespace') or 'unknown'}"
        f"/{incident.get('resource_kind') or 'unknown'}"
        f"/{incident.get('resource_name') or 'unknown'}"
    )


def _apply_created_at_window(
    statement: Select[Any],
    table: Any,
    since: datetime | None,
    until: datetime | None,
) -> Select[Any]:
    if since is not None:
        statement = statement.where(table.c.created_at >= since)
    if until is not None:
        statement = statement.where(table.c.created_at < until)
    return statement


def _apply_keyset_or_offset(
    statement: Select[Any],
    table: Any,
    cursor: tuple[datetime, int] | None,
    offset: int,
) -> Select[Any]:
    if cursor is None:
        return statement.offset(offset)
    created_at, row_id = cursor
    # 최신순 정렬에서 cursor 다음 페이지는 더 오래된 시각 또는 같은 시각의 더 작은 id 다.
    return statement.where(
        or_(
            table.c.created_at < created_at,
            and_(table.c.created_at == created_at, table.c.id < row_id),
        )
    )


def _serialize_created_at(row: Any) -> JsonObject:
    item = dict(row)
    item["created_at"] = iso_or_none(item.get("created_at"))
    return item
