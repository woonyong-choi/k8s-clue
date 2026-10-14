"""target 도메인 repository — agent lease와 evidence dedupe."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, cast, delete, func, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert

from domains.identity.models import ClusterRegistration
from domains.target.evidence_jobs import (
    DEFAULT_EVIDENCE_JOB_LEASE_SECONDS,
    DEFAULT_PENDING_EVIDENCE_EVENT_TTL_SECONDS,
    EVIDENCE_JOB_STATUS_COMPLETED,
    EVIDENCE_JOB_STATUS_FAILED,
    EVIDENCE_JOB_STATUS_LEASED,
    EVIDENCE_JOB_STATUS_QUEUED,
    PENDING_EVIDENCE_EVENT_ID_PREFIX,
    aggregate_evidence_payload,
    evidence_job_id,
    evidence_key,
    normalize_evidence_provider_result,
)
from domains.target.models import (
    AgentPolicyRecord,
    AgentPolicyStatusRecord,
    AgentReconcileStatusRecord,
    ClusterAgentStatusRecord,
    EvidenceJob,
    EvidenceWindow,
    TargetDesiredState,
    TargetReconcileRecord,
)
from packages.config.settings import env
from packages.contracts.event_bus.interfaces import EventEnvelope, JsonObject
from packages.contracts.identity import ClusterRegistrationStatus
from packages.contracts.target import TargetComponent, TargetDesiredStateStatus
from packages.contracts.timeline import TimelineEvent
from packages.storage.engine import DatabaseConnection, iso_or_none
from packages.storage.schema import EventModel, OutboxModel

if TYPE_CHECKING:
    from domains.target.policy_upgrade import TargetUpgradePlan

AGENT_STATUS_RETENTION_SECONDS_ENV = "AGENT_STATUS_RETENTION_SECONDS"
DEFAULT_AGENT_STATUS_RETENTION_SECONDS = 3600


def cluster_policy_lock_key(workspace_id: str, cluster_id: str) -> int:
    raw = f"cluster-policy\0{workspace_id}\0{cluster_id}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], byteorder="big", signed=True)


def evidence_window_lock_key(evidence_key_value: str) -> int:
    raw = f"evidence-window\0{evidence_key_value}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], byteorder="big", signed=True)


def agent_status_retention_seconds() -> int:
    """종료된 agent pod 상태를 보존할 최대 시간을 반환한다."""
    try:
        configured = int(
            env(
                AGENT_STATUS_RETENTION_SECONDS_ENV,
                str(DEFAULT_AGENT_STATUS_RETENTION_SECONDS),
            )
        )
    except ValueError:
        return DEFAULT_AGENT_STATUS_RETENTION_SECONDS
    return max(300, configured)


class TargetAgentRepository(DatabaseConnection):
    def lock_cluster_policy_for_update(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        conn: Any,
    ) -> None:
        conn.execute(
            select(func.pg_advisory_xact_lock(cluster_policy_lock_key(workspace_id, cluster_id)))
        )

    def list_target_runtime_upgrade_candidates(
        self,
        *,
        after_id: int,
        limit: int,
    ) -> list[JsonObject]:
        """Read one keyset page with policy, desired state, and latest RBAC proof.

        The fixed query count avoids a per-cluster policy/status lookup while keeping
        memory bounded for installations with many workspaces.
        """

        registration = ClusterRegistration.__table__
        page_size = max(1, min(limit, 500))
        registration_statement = (
            select(
                registration.c.id,
                registration.c.workspace_id,
                registration.c.cluster_id,
                registration.c.name,
                registration.c.environment,
                registration.c.status,
                registration.c.settings,
            )
            .where(
                registration.c.id > after_id,
                registration.c.status.in_(
                    (
                        ClusterRegistrationStatus.PENDING_INSTALL.value,
                        ClusterRegistrationStatus.INSTALL_APPLIED.value,
                        ClusterRegistrationStatus.INSTALL_FAILED.value,
                        ClusterRegistrationStatus.REGISTERED.value,
                    )
                ),
            )
            .order_by(registration.c.id)
            .limit(page_size)
        )
        with self.connection() as conn:
            registrations = [dict(row) for row in conn.execute(registration_statement).mappings()]
            if not registrations:
                return []

            identities = [
                (str(item["workspace_id"]), str(item["cluster_id"])) for item in registrations
            ]
            policy = AgentPolicyRecord.__table__
            policies = {
                (str(row["workspace_id"]), str(row["cluster_id"])): dict(row["policy"])
                for row in conn.execute(
                    select(
                        policy.c.workspace_id,
                        policy.c.cluster_id,
                        policy.c.policy,
                    ).where(tuple_(policy.c.workspace_id, policy.c.cluster_id).in_(identities))
                ).mappings()
            }

            desired = TargetDesiredState.__table__
            desired_by_cluster: dict[tuple[str, str], list[JsonObject]] = {}
            for row in conn.execute(
                select(
                    desired.c.workspace_id,
                    desired.c.cluster_id,
                    desired.c.component,
                    desired.c.namespace,
                    desired.c.version,
                    desired.c.status,
                    desired.c.updated_by,
                    desired.c.spec,
                )
                .where(tuple_(desired.c.workspace_id, desired.c.cluster_id).in_(identities))
                .order_by(desired.c.workspace_id, desired.c.cluster_id, desired.c.component)
            ).mappings():
                identity = (str(row["workspace_id"]), str(row["cluster_id"]))
                desired_by_cluster.setdefault(identity, []).append(dict(row))

            status = AgentPolicyStatusRecord.__table__
            ranked_status = (
                select(
                    status.c.workspace_id,
                    status.c.cluster_id,
                    status.c.generation,
                    status.c.status,
                    status.c.details,
                    func.row_number()
                    .over(
                        partition_by=(status.c.workspace_id, status.c.cluster_id),
                        order_by=status.c.id.desc(),
                    )
                    .label("position"),
                )
                .where(tuple_(status.c.workspace_id, status.c.cluster_id).in_(identities))
                .subquery("latest_target_policy_status")
            )
            statuses = {
                (str(row["workspace_id"]), str(row["cluster_id"])): dict(row)
                for row in conn.execute(
                    select(ranked_status).where(ranked_status.c.position == 1)
                ).mappings()
            }

        results: list[JsonObject] = []
        for item in registrations:
            identity = (str(item["workspace_id"]), str(item["cluster_id"]))
            results.append(
                {
                    "registration": item,
                    "policy": policies.get(identity),
                    "desired_states": desired_by_cluster.get(identity, []),
                    "policy_status": statuses.get(identity),
                }
            )
        return results

    def apply_target_runtime_upgrade(self, plan: TargetUpgradePlan) -> None:
        """Apply one planned upgrade with policy generation compare-and-swap."""

        from domains.target.policy_upgrade import UPGRADE_ACTOR

        if not plan.changed or plan.policy is None:
            return
        policy = AgentPolicyRecord.__table__
        registration = ClusterRegistration.__table__
        with self.connection() as conn:
            if plan.next_generation > plan.current_generation:
                values = {
                    "generation": plan.next_generation,
                    "policy": plan.policy.model_dump(),
                    "updated_at": func.now(),
                }
                if plan.policy_existed:
                    changed = conn.execute(
                        update(policy)
                        .where(
                            policy.c.workspace_id == plan.workspace_id,
                            policy.c.cluster_id == plan.cluster_id,
                            policy.c.generation == plan.current_generation,
                        )
                        .values(**values)
                        .returning(policy.c.generation)
                    ).scalar_one_or_none()
                else:
                    changed = conn.execute(
                        pg_insert(policy)
                        .values(
                            workspace_id=plan.workspace_id,
                            cluster_id=plan.cluster_id,
                            **values,
                        )
                        .on_conflict_do_nothing(
                            index_elements=[policy.c.workspace_id, policy.c.cluster_id]
                        )
                        .returning(policy.c.generation)
                    ).scalar_one_or_none()
                if changed != plan.next_generation:
                    raise RuntimeError("target policy generation changed during upgrade")

            if plan.settings_patch:
                updated_registration = conn.execute(
                    update(registration)
                    .where(
                        registration.c.id == plan.registration_id,
                        registration.c.workspace_id == plan.workspace_id,
                        registration.c.cluster_id == plan.cluster_id,
                        registration.c.status.in_(
                            (
                                ClusterRegistrationStatus.PENDING_INSTALL.value,
                                ClusterRegistrationStatus.INSTALL_APPLIED.value,
                                ClusterRegistrationStatus.INSTALL_FAILED.value,
                                ClusterRegistrationStatus.REGISTERED.value,
                            )
                        ),
                    )
                    .values(
                        settings=registration.c.settings.op("||")(cast(plan.settings_patch, JSONB)),
                        updated_at=func.now(),
                    )
                    .returning(registration.c.id)
                ).scalar_one_or_none()
                if updated_registration != plan.registration_id:
                    raise RuntimeError("target registration changed during upgrade")

            self.upsert_target_desired_states(
                plan.workspace_id,
                plan.cluster_id,
                [
                    item
                    for item in plan.desired_states
                    if item.get("component") == TargetComponent.CLUSTER_AGENT.value
                ],
                UPGRADE_ACTOR,
                preserve_spec=True,
            )

    def save_cluster_agent_status(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        agent_id: str,
        capabilities: list[str] | None,
        status: str = "connected",
        details: JsonObject | None = None,
    ) -> JsonObject:
        table = ClusterAgentStatusRecord.__table__
        normalized_capabilities = (
            list(dict.fromkeys(capabilities)) if capabilities is not None else None
        )
        update_values: dict[str, Any] = {
            "status": status,
            "last_seen_at": func.now(),
            "updated_at": func.now(),
        }
        if details:
            update_values["details"] = table.c.details.op("||")(cast(details, JSONB))
        if normalized_capabilities is not None:
            update_values["capabilities"] = normalized_capabilities
        statement = (
            pg_insert(table)
            .values(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                agent_id=agent_id,
                status=status,
                capabilities=normalized_capabilities or [],
                details=details or {},
                last_seen_at=func.now(),
                updated_at=func.now(),
            )
            .on_conflict_do_update(
                index_elements=[table.c.workspace_id, table.c.cluster_id, table.c.agent_id],
                set_=update_values,
            )
            .returning(table)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().one()
            stale_before = datetime.now(UTC) - timedelta(seconds=agent_status_retention_seconds())
            conn.execute(
                delete(table).where(
                    table.c.workspace_id == workspace_id,
                    table.c.cluster_id == cluster_id,
                    table.c.agent_id != agent_id,
                    table.c.last_seen_at < stale_before,
                )
            )
        return self.serialize_cluster_agent_status(dict(row))

    def list_cluster_agent_statuses(
        self,
        workspace_id: str,
        cluster_id: str,
    ) -> list[JsonObject]:
        table = ClusterAgentStatusRecord.__table__
        statement = (
            select(table)
            .where(table.c.workspace_id == workspace_id, table.c.cluster_id == cluster_id)
            .order_by(table.c.last_seen_at.desc(), table.c.agent_id)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [self.serialize_cluster_agent_status(dict(row)) for row in rows]

    def latest_cluster_agent_statuses(
        self,
        workspace_id: str,
        cluster_ids: set[str] | None,
    ) -> dict[str, JsonObject]:
        if cluster_ids is not None and not cluster_ids:
            return {}

        table = ClusterAgentStatusRecord.__table__
        statement = select(table).where(table.c.workspace_id == workspace_id)
        if cluster_ids is not None:
            statement = statement.where(table.c.cluster_id.in_(cluster_ids))
        statement = statement.order_by(table.c.cluster_id, table.c.last_seen_at.desc())

        latest: dict[str, JsonObject] = {}
        with self.connection() as conn:
            for row in conn.execute(statement).mappings():
                cluster_id = str(row["cluster_id"])
                if cluster_id not in latest:
                    latest[cluster_id] = self.serialize_cluster_agent_status(dict(row))
        return latest

    def upsert_cluster_policy(
        self,
        workspace_id: str,
        cluster_id: str,
        policy: JsonObject,
        *,
        conn: Any | None = None,
    ) -> JsonObject:
        generation = int(policy.get("generation", 1))
        table = AgentPolicyRecord.__table__
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            self.lock_cluster_policy_for_update(workspace_id, cluster_id, conn=connection)
            existing = (
                connection.execute(
                    select(table.c.generation).where(
                        table.c.workspace_id == workspace_id,
                        table.c.cluster_id == cluster_id,
                    )
                )
                .mappings()
                .first()
            )
            if existing and int(existing["generation"]) >= generation:
                raise ValueError("policy generation must be greater than the current generation")

            statement = (
                pg_insert(table)
                .values(
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    generation=generation,
                    policy=policy,
                    updated_at=func.now(),
                )
                .on_conflict_do_update(
                    index_elements=[table.c.workspace_id, table.c.cluster_id],
                    set_={
                        "generation": generation,
                        "policy": policy,
                        "updated_at": func.now(),
                    },
                )
                .returning(table.c.policy)
            )
            row = connection.execute(statement).mappings().one()
        return dict(row["policy"])

    def get_cluster_policy(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        conn: Any | None = None,
    ) -> JsonObject | None:
        table = AgentPolicyRecord.__table__
        statement = select(table.c.policy).where(
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
        )
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            row = connection.execute(statement).mappings().first()
        return dict(row["policy"]) if row else None

    def save_agent_policy_status(
        self,
        workspace_id: str,
        payload: JsonObject,
    ) -> None:
        self._append_agent_status_if_changed(
            AgentPolicyStatusRecord.__table__, workspace_id, payload
        )

    def save_agent_reconcile_status(
        self,
        workspace_id: str,
        payload: JsonObject,
    ) -> None:
        self._append_agent_status_if_changed(
            AgentReconcileStatusRecord.__table__, workspace_id, payload
        )

    def _append_agent_status_if_changed(
        self,
        table: Any,
        workspace_id: str,
        payload: JsonObject,
    ) -> None:
        """무변화 반복 보고의 append 를 생략한다 — 최신 행과 완전 동일하면 skip.

        이 테이블들의 조회는 전부 최신 행 기준(row_number over id desc)이라
        동일 내용 재기록은 정보를 더하지 않고 저장량만 늘린다(같은 generation·
        unchanged 가 주기 보고마다 수만 건 누적). 내용이 하나라도 다르면 그대로
        append 되어 변화 이력은 보존된다.
        """
        values = {
            "workspace_id": workspace_id,
            "cluster_id": payload["cluster_id"],
            "generation": payload["generation"],
            "status": payload["status"],
            "message": payload.get("message", ""),
            "details": payload.get("details", {}),
        }
        with self.connection() as conn:
            latest = (
                conn.execute(
                    select(table.c.generation, table.c.status, table.c.message, table.c.details)
                    .where(
                        table.c.workspace_id == workspace_id,
                        table.c.cluster_id == str(values["cluster_id"]),
                    )
                    .order_by(table.c.id.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            if (
                latest is not None
                and int(latest["generation"]) == int(values["generation"])
                and str(latest["status"]) == str(values["status"])
                and str(latest["message"]) == str(values["message"])
                and dict(latest["details"] or {}) == dict(values["details"] or {})
            ):
                return
            conn.execute(pg_insert(table).values(**values))

    def upsert_target_desired_states(
        self,
        workspace_id: str,
        cluster_id: str,
        components: list[JsonObject],
        updated_by: str | None,
        *,
        preserve_spec: bool = False,
    ) -> list[JsonObject]:
        table = TargetDesiredState.__table__
        records: list[JsonObject] = []
        with self.connection() as conn:
            for component in components:
                update_values: dict[str, Any] = {
                    "namespace": component["namespace"],
                    "version": component["version"],
                    "status": TargetDesiredStateStatus.ACTIVE.value,
                    "updated_by": updated_by,
                    "updated_at": func.now(),
                }
                if not preserve_spec:
                    update_values["spec"] = component["spec"]
                statement = (
                    pg_insert(table)
                    .values(
                        workspace_id=workspace_id,
                        cluster_id=cluster_id,
                        component=component["component"],
                        namespace=component["namespace"],
                        version=component["version"],
                        status=TargetDesiredStateStatus.ACTIVE.value,
                        updated_by=updated_by,
                        spec=component["spec"],
                        updated_at=func.now(),
                    )
                    .on_conflict_do_update(
                        index_elements=[
                            table.c.workspace_id,
                            table.c.cluster_id,
                            table.c.component,
                        ],
                        set_=update_values,
                    )
                    .returning(
                        table.c.workspace_id,
                        table.c.cluster_id,
                        table.c.component,
                        table.c.namespace,
                        table.c.version,
                        table.c.status,
                        table.c.updated_by,
                        table.c.spec,
                    )
                )
                row = conn.execute(statement).mappings().one()
                records.append(dict(row))
        return records

    def list_target_desired_states(self, workspace_id: str, cluster_id: str) -> list[JsonObject]:
        table = TargetDesiredState.__table__
        statement = (
            select(
                table.c.workspace_id,
                table.c.cluster_id,
                table.c.component,
                table.c.namespace,
                table.c.version,
                table.c.status,
                table.c.updated_by,
                table.c.spec,
            )
            .where(table.c.workspace_id == workspace_id, table.c.cluster_id == cluster_id)
            .order_by(table.c.component)
        )
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]

    def record_target_reconcile_result(self, payload: JsonObject) -> JsonObject:
        table = TargetReconcileRecord.__table__
        reconcile_id = str(payload.get("reconcile_id") or uuid.uuid4())
        statement = (
            pg_insert(table)
            .values(
                reconcile_id=reconcile_id,
                workspace_id=payload["workspace_id"],
                cluster_id=payload["cluster_id"],
                desired_state_version=payload["desired_state_version"],
                status=payload["status"],
                drifted=payload["drifted"],
                applied=payload["applied"],
                message=payload["message"],
                details=payload.get("details", {}),
                updated_at=func.now(),
            )
            .returning(
                table.c.reconcile_id,
                table.c.workspace_id,
                table.c.cluster_id,
                table.c.desired_state_version,
                table.c.status,
                table.c.drifted,
                table.c.applied,
                table.c.message,
                table.c.details,
            )
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().one()
        return dict(row)

    def queue_evidence_jobs(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        source_id: str,
        window_start: str,
        provider_keys: list[str],
        failure_policy: str,
        max_attempts: int,
        policy_generation: int,
        provider_policies: dict[str, JsonObject],
    ) -> JsonObject:
        table = EvidenceJob.__table__
        parent_key = evidence_key(workspace_id, cluster_id, source_id, window_start)
        job_ids: list[str] = []
        with self.connection() as conn:
            for provider_key in dict.fromkeys(provider_keys):
                job_id = evidence_job_id(
                    workspace_id,
                    cluster_id,
                    source_id,
                    window_start,
                    provider_key,
                )
                statement = (
                    pg_insert(table)
                    .values(
                        job_id=job_id,
                        evidence_key=parent_key,
                        workspace_id=workspace_id,
                        cluster_id=cluster_id,
                        source_id=source_id,
                        provider_key=provider_key,
                        window_start=window_start,
                        policy_generation=policy_generation,
                        provider_policy=provider_policies.get(provider_key, {}),
                        status=EVIDENCE_JOB_STATUS_QUEUED,
                        lease_id=None,
                        agent_id=None,
                        leased_until=None,
                        attempt_count=0,
                        max_attempts=max_attempts,
                        failure_policy=failure_policy,
                        result=None,
                        error=None,
                        updated_at=func.now(),
                    )
                    .on_conflict_do_nothing(index_elements=[table.c.job_id])
                    .returning(table.c.job_id)
                )
                row = conn.execute(statement).mappings().first()
                if row:
                    job_ids.append(str(row["job_id"]))
        return {
            "accepted": True,
            "evidence_key": parent_key,
            "queued": len(job_ids),
            "job_ids": job_ids,
        }

    async def lease_evidence_job(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        provider_key: str,
        agent_id: str,
        lease_seconds: int = DEFAULT_EVIDENCE_JOB_LEASE_SECONDS,
    ) -> JsonObject | None:
        table = EvidenceJob.__table__
        lease_id = str(uuid.uuid4())
        leased_until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        columns = (
            table.c.job_id,
            table.c.evidence_key,
            table.c.workspace_id,
            table.c.cluster_id,
            table.c.source_id,
            table.c.provider_key,
            table.c.window_start,
            table.c.policy_generation,
            table.c.provider_policy,
            table.c.status,
            table.c.lease_id,
            table.c.agent_id,
            table.c.leased_until,
            table.c.attempt_count,
            table.c.max_attempts,
            table.c.failure_policy,
        )
        available = or_(
            table.c.status == EVIDENCE_JOB_STATUS_QUEUED,
            (table.c.status == EVIDENCE_JOB_STATUS_LEASED) & (table.c.leased_until < func.now()),
        )
        candidate = (
            select(table.c.job_id)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.provider_key == provider_key,
                available,
                # 재임대도 attempt 소모다 — 명시적 실패 보고 없이 lease 만 만료되는
                # 장애(프로세스 사망·hang)가 무한 재임대로 이어지지 않도록 상한을
                # lease 시점에 강제한다. 소진된 잡은 janitor 가 FAILED 로 종결한다.
                table.c.attempt_count < table.c.max_attempts,
            )
            .order_by(table.c.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        async with self.async_connection() as conn:
            statement = (
                update(table)
                .where(table.c.job_id == candidate)
                .values(
                    status=EVIDENCE_JOB_STATUS_LEASED,
                    lease_id=lease_id,
                    agent_id=agent_id,
                    leased_until=leased_until,
                    attempt_count=table.c.attempt_count + 1,
                    error=None,
                    updated_at=func.now(),
                )
                .returning(*columns)
            )
            row = (await conn.execute(statement)).mappings().first()
        return self.serialize_evidence_job(dict(row)) if row else None

    def fail_exhausted_evidence_jobs(self, *, limit: int = 200) -> list[str]:
        """attempt 소진 + lease 만료 잡을 FAILED 로 종결하고 evidence_key 를 반환.

        lease 조건에 attempt 상한이 걸리면서, 상한을 소진한 채 lease 가 만료된
        잡은 어떤 에이전트도 다시 가져가지 못한다. 명시적 실패 보고 없이 죽은
        경우를 janitor 가 종결해야 window 집계가 failure_policy 에 따라 진행된다.
        호출자는 반환된 key 마다 집계·발행 시도를 트리거한다.
        """
        table = EvidenceJob.__table__
        exhausted = (
            select(table.c.job_id)
            .where(
                table.c.status == EVIDENCE_JOB_STATUS_LEASED,
                table.c.leased_until < func.now(),
                table.c.attempt_count >= table.c.max_attempts,
            )
            .order_by(table.c.job_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("exhausted_evidence_jobs")
        )
        statement = (
            update(table)
            .where(table.c.job_id.in_(select(exhausted.c.job_id)))
            .values(
                status=EVIDENCE_JOB_STATUS_FAILED,
                result=None,
                error="lease expired; retry attempts exhausted",
                updated_at=func.now(),
            )
            .returning(table.c.evidence_key)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).all()
        return sorted({str(row[0]) for row in rows})

    def complete_evidence_job(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        job_id: str,
        lease_id: str,
        agent_id: str,
        status: str,
        result: JsonObject,
        error: str,
    ) -> JsonObject | None:
        table = EvidenceJob.__table__
        with self.connection() as conn:
            active = (
                conn.execute(
                    select(
                        table.c.job_id,
                        table.c.evidence_key,
                        table.c.provider_key,
                        table.c.attempt_count,
                        table.c.max_attempts,
                    )
                    .where(
                        table.c.job_id == job_id,
                        table.c.workspace_id == workspace_id,
                        table.c.cluster_id == cluster_id,
                        table.c.lease_id == lease_id,
                        table.c.agent_id == agent_id,
                        # lease 만료 후에도 아무도 재임대하지 않았다면(같은 lease_id 가
                        # 그 증거) 성실한 늦은 결과를 받아준다. 재임대가 일어났다면
                        # lease_id 가 바뀌어 있어 자동 거부되므로 이중 수용은 구조적으로
                        # 불가능하다 — 시간 초과라는 이유만으로 정상 수집을 버리지 않는다.
                        table.c.status == EVIDENCE_JOB_STATUS_LEASED,
                    )
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if active is None:
                existing = (
                    conn.execute(
                        select(
                            table.c.job_id,
                            table.c.evidence_key,
                            table.c.provider_key,
                            table.c.source_id,
                            table.c.window_start,
                            table.c.status,
                        ).where(
                            table.c.job_id == job_id,
                            table.c.workspace_id == workspace_id,
                            table.c.cluster_id == cluster_id,
                            table.c.lease_id == lease_id,
                            table.c.agent_id == agent_id,
                            table.c.status.in_(
                                (
                                    EVIDENCE_JOB_STATUS_COMPLETED,
                                    EVIDENCE_JOB_STATUS_FAILED,
                                    EVIDENCE_JOB_STATUS_QUEUED,
                                )
                            ),
                        )
                    )
                    .mappings()
                    .first()
                )
                return dict(existing) if existing else None

            next_status = EVIDENCE_JOB_STATUS_COMPLETED
            if status == EVIDENCE_JOB_STATUS_FAILED:
                next_status = (
                    EVIDENCE_JOB_STATUS_FAILED
                    if int(active["attempt_count"]) >= int(active["max_attempts"])
                    else EVIDENCE_JOB_STATUS_QUEUED
                )

            row = (
                conn.execute(
                    update(table)
                    .where(table.c.job_id == job_id)
                    .values(
                        status=next_status,
                        result=(
                            normalize_evidence_provider_result(str(active["provider_key"]), result)
                            if next_status == EVIDENCE_JOB_STATUS_COMPLETED
                            else None
                        ),
                        error=error or None,
                        updated_at=func.now(),
                    )
                    .returning(
                        table.c.job_id,
                        table.c.evidence_key,
                        table.c.provider_key,
                        table.c.source_id,
                        table.c.window_start,
                        table.c.status,
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)

    def evidence_payload_if_ready(self, evidence_key_value: str) -> JsonObject | None:
        table = EvidenceJob.__table__
        statement = (
            select(
                table.c.evidence_key,
                table.c.workspace_id,
                table.c.cluster_id,
                table.c.source_id,
                table.c.provider_key,
                table.c.provider_policy,
                table.c.window_start,
                table.c.status,
                table.c.failure_policy,
                table.c.agent_id,
                table.c.result,
            )
            .where(table.c.evidence_key == evidence_key_value)
            .order_by(table.c.provider_key)
        )
        with self.connection() as conn:
            rows = [dict(row) for row in conn.execute(statement).mappings()]
        return aggregate_evidence_payload(rows)

    def list_evidence_jobs_for_window(
        self,
        evidence_key_value: str,
        workspace_id: str,
    ) -> list[JsonObject]:
        table = EvidenceJob.__table__
        statement = (
            select(
                table.c.job_id,
                table.c.provider_key,
                table.c.status,
                table.c.error,
                table.c.attempt_count,
                table.c.max_attempts,
            )
            .where(
                table.c.evidence_key == evidence_key_value,
                table.c.workspace_id == workspace_id,
            )
            .order_by(table.c.provider_key)
        )
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]

    def evidence_job_status_counts(self) -> dict[str, int]:
        table = EvidenceJob.__table__
        statement = select(table.c.status, func.count().label("count")).group_by(table.c.status)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return {row["status"]: int(row["count"]) for row in rows}

    def oldest_evidence_job_age_seconds(self, status: str) -> float:
        table = EvidenceJob.__table__
        statement = select(func.extract("epoch", func.now() - func.min(table.c.created_at))).where(
            table.c.status == status
        )
        with self.connection() as conn:
            age = conn.execute(statement).scalar()
        return float(age or 0)

    def serialize_evidence_job(self, row: JsonObject) -> JsonObject:
        item = dict(row)
        item["leased_until"] = iso_or_none(item.get("leased_until"))
        return item

    def serialize_cluster_agent_status(self, row: JsonObject) -> JsonObject:
        item = dict(row)
        item["last_seen_at"] = iso_or_none(item.get("last_seen_at"))
        item["created_at"] = iso_or_none(item.get("created_at"))
        item["updated_at"] = iso_or_none(item.get("updated_at"))
        return item

    def get_evidence_window(self, evidence_key: str) -> JsonObject | None:
        table = EvidenceWindow.__table__
        statement = select(
            table.c.event_id,
            table.c.correlation_id,
            table.c.updated_at,
        ).where(table.c.evidence_key == evidence_key)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return dict(row) if row else None

    def get_evidence_window_payload(self, evidence_key: str) -> JsonObject | None:
        table = EvidenceWindow.__table__
        statement = select(table.c.payload).where(table.c.evidence_key == evidence_key)
        with self.connection() as conn:
            payload = conn.execute(statement).scalar_one_or_none()
        return payload if isinstance(payload, dict) else None

    def upsert_rca_enriched_evidence_window(
        self,
        *,
        evidence_key: str,
        workspace_id: str,
        cluster_id: str,
        correlation_id: str,
        window_start: str,
        source_id: str,
        agent_id: str | None,
        payload: JsonObject,
    ) -> bool:
        """Persist the exact joined payload exposed by clickable RCA references."""

        table = EvidenceWindow.__table__
        insert = pg_insert(table).values(
            evidence_key=evidence_key,
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            source_id=source_id,
            window_start=window_start,
            agent_id=agent_id,
            event_id=f"derived:{correlation_id}",
            correlation_id=correlation_id,
            payload=payload,
            updated_at=func.now(),
        )
        statement = (
            insert.on_conflict_do_update(
                index_elements=[table.c.evidence_key],
                set_={
                    "source_id": insert.excluded.source_id,
                    "window_start": insert.excluded.window_start,
                    "agent_id": insert.excluded.agent_id,
                    "payload": insert.excluded.payload,
                    "updated_at": func.now(),
                },
                where=and_(
                    table.c.workspace_id == insert.excluded.workspace_id,
                    table.c.cluster_id == insert.excluded.cluster_id,
                    table.c.correlation_id == insert.excluded.correlation_id,
                ),
            )
            .returning(table.c.evidence_key)
        )
        with self.connection() as conn:
            saved_key = conn.execute(statement).scalar_one_or_none()
        return saved_key == evidence_key

    def list_aligned_evidence_window_payloads(
        self,
        workspace_id: str,
        cluster_id: str,
        observed_at: str,
        *,
        exclude_source_id: str,
        before_seconds: int = 600,
        after_seconds: int = 60,
        limit: int = 12,
    ) -> list[JsonObject]:
        """Read bounded adjacent windows inside one exact tenant and cluster."""

        try:
            observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return []
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        before = max(0, min(int(before_seconds), 3600))
        after = max(0, min(int(after_seconds), 300))
        max_rows = max(1, min(int(limit), 50))
        table = EvidenceWindow.__table__
        statement = (
            select(
                table.c.evidence_key,
                table.c.source_id,
                table.c.window_start,
                table.c.agent_id,
                table.c.payload,
                table.c.updated_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.source_id != exclude_source_id,
                table.c.agent_id.is_not(None),
                table.c.updated_at >= observed - timedelta(seconds=before),
                table.c.updated_at <= observed + timedelta(seconds=after),
            )
            .order_by(table.c.updated_at.desc(), table.c.evidence_key.desc())
            .limit(max_rows)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows if isinstance(row.get("payload"), dict)]

    def list_aligned_alertmanager_window_payloads(
        self,
        workspace_id: str,
        cluster_id: str,
        observed_at: str,
        *,
        source_id: str,
        before_seconds: int = 60,
        after_seconds: int = 600,
        limit: int = 12,
    ) -> list[JsonObject]:
        """Read adjacent Alertmanager windows for a later Agent evidence window."""

        try:
            observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return []
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        before = max(0, min(int(before_seconds), 300))
        after = max(0, min(int(after_seconds), 3600))
        max_rows = max(1, min(int(limit), 50))
        table = EvidenceWindow.__table__
        statement = (
            select(
                table.c.evidence_key,
                table.c.source_id,
                table.c.window_start,
                table.c.payload,
                table.c.updated_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.source_id == source_id,
                table.c.updated_at >= observed - timedelta(seconds=before),
                table.c.updated_at <= observed + timedelta(seconds=after),
            )
            .order_by(table.c.updated_at.desc(), table.c.evidence_key.desc())
            .limit(max_rows)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows if isinstance(row.get("payload"), dict)]

    def get_evidence_window_payload_for_workspace(
        self, workspace_id: str, evidence_key: str
    ) -> JsonObject | None:
        table = EvidenceWindow.__table__
        statement = select(table.c.payload).where(
            table.c.workspace_id == workspace_id,
            table.c.evidence_key == evidence_key,
        )
        with self.connection() as conn:
            payload = conn.execute(statement).scalar_one_or_none()
        return payload if isinstance(payload, dict) else None

    def list_evidence_windows_for_workspace(
        self,
        workspace_id: str,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[JsonObject]:
        table = EvidenceWindow.__table__
        statement = (
            select(
                table.c.evidence_key,
                table.c.workspace_id,
                table.c.cluster_id,
                table.c.source_id,
                table.c.window_start,
                table.c.agent_id,
                table.c.correlation_id,
                table.c.payload,
                table.c.created_at,
                table.c.updated_at,
            )
            .where(table.c.workspace_id == workspace_id)
            .order_by(table.c.updated_at.desc(), table.c.evidence_key.desc())
            .limit(limit)
            .offset(offset)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def record_evidence_event_once(
        self,
        *,
        evidence_key: str,
        workspace_id: str,
        cluster_id: str,
        source_id: str,
        window_start: str,
        agent_id: str | None,
        event_envelope: EventEnvelope,
        payload: JsonObject,
        timeline_events: Sequence[TimelineEvent] = (),
        inventory_payload: JsonObject | None = None,
    ) -> JsonObject:
        """Record one evidence window once, with its Timeline facts in the same transaction.

        ``timeline_events`` and ``inventory_payload`` are optional so existing callers keep
        their behaviour.  When given, the snapshot write and the ledger append join this
        unit of work: either the window, outbox event, snapshot and ledger facts all
        commit, or none does.  A re-received key returns before any write, so a repeat can
        never add a second snapshot or ledger fact.
        """
        window_table = EvidenceWindow.__table__
        event_table = EventModel.__table__
        outbox_table = OutboxModel.__table__
        with self.unit_of_work() as conn:
            inserted = (
                conn.execute(
                    pg_insert(window_table)
                    .values(
                        evidence_key=evidence_key,
                        workspace_id=workspace_id,
                        cluster_id=cluster_id,
                        source_id=source_id,
                        window_start=window_start,
                        agent_id=agent_id,
                        event_id=event_envelope.event_id,
                        correlation_id=event_envelope.correlation_id,
                        payload=payload,
                        updated_at=func.now(),
                    )
                    .on_conflict_do_nothing(index_elements=[window_table.c.evidence_key])
                    .returning(window_table.c.event_id, window_table.c.correlation_id)
                )
                .mappings()
                .first()
            )
            if inserted is None:
                existing = (
                    conn.execute(
                        select(window_table.c.event_id, window_table.c.correlation_id).where(
                            window_table.c.evidence_key == evidence_key
                        )
                    )
                    .mappings()
                    .one()
                )
                return {"duplicate": True, **dict(existing)}

            trusted_envelope = replace(event_envelope, workspace_id=workspace_id)
            self.stage_event_envelope(conn, event_table, outbox_table, trusted_envelope)
            # 집계 payload 가 window 행에 확정된 순간 provider 원문(result)은 더
            # 이상 읽히지 않는다 — evidence_payload_if_ready 는 window 생성 전
            # 단계에서만 호출되고, lease/complete/목록 조회는 result 를 반환하지
            # 않는다. 같은 트랜잭션에서 비워 JSONB/TOAST 중복 보존을 제거한다
            # (동일 payload 가 window·events·outbox 에 이미 3중 저장됨).
            job_table = EvidenceJob.__table__
            conn.execute(
                update(job_table)
                .where(
                    job_table.c.evidence_key == evidence_key,
                    job_table.c.workspace_id == workspace_id,
                    job_table.c.result.is_not(None),
                )
                .values(result=None, updated_at=func.now())
            )
            if timeline_events:
                self._append_timeline_events_in_transaction(timeline_events)
            if inventory_payload is not None:
                self._record_inventory_in_transaction(
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    agent_id=agent_id,
                    payload=inventory_payload,
                )
        return {"duplicate": False, **dict(inserted)}

    def _record_inventory_in_transaction(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        agent_id: str | None,
        payload: JsonObject,
    ) -> None:
        # Inventory and ledger repositories are composed into Database beside this one.
        save = getattr(self, "save_inventory_snapshot_with_timeline", None)
        append = getattr(self, "append_timeline_events", None)
        if not callable(save) or not callable(append):
            raise TypeError("inventory and timeline ledger are not composed into this repository")
        save(
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            agent_id=agent_id or "cluster-agent",
            payload=payload,
            append_timeline_events=append,
        )

    def _append_timeline_events_in_transaction(self, events: Sequence[TimelineEvent]) -> None:
        # The ledger repository is composed into Database beside this one; it joins the
        # active unit of work, so a missing ledger must fail the whole recording.
        append = getattr(self, "append_timeline_events", None)
        if not callable(append):
            raise TypeError("timeline ledger is not composed into this repository")
        append(events)

    def list_evidence_windows_by_correlation(
        self,
        workspace_id: str,
        cluster_id: str,
        correlation_id: str,
        *,
        limit: int,
    ) -> list[JsonObject]:
        """List evidence window references of one incident without their payloads."""
        table = EvidenceWindow.__table__
        statement = (
            select(
                table.c.evidence_key,
                table.c.source_id,
                table.c.window_start,
                table.c.event_id,
                table.c.updated_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.correlation_id == correlation_id,
            )
            .order_by(table.c.updated_at.asc(), table.c.evidence_key.asc())
            .limit(limit)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def rotate_alertmanager_evidence_window(
        self,
        *,
        evidence_key: str,
        expected_event_id: str,
        expected_correlation_id: str,
        workspace_id: str,
        cluster_id: str,
        source_id: str,
        window_start: str,
        agent_id: str | None,
        event_envelope: EventEnvelope,
        payload: JsonObject,
    ) -> JsonObject:
        """Atomically move a closed/orphaned Alertmanager key to a new incident.

        The stable key remains the live dedupe pointer. Its previous value is
        archived under a deterministic history key so incident evidence remains
        inspectable without letting repeats reuse a dead correlation.
        """

        window_table = EvidenceWindow.__table__
        event_table = EventModel.__table__
        outbox_table = OutboxModel.__table__
        trusted_envelope = replace(event_envelope, workspace_id=workspace_id)
        new_values = {
            "workspace_id": workspace_id,
            "cluster_id": cluster_id,
            "source_id": source_id,
            "window_start": window_start,
            "agent_id": agent_id,
            "event_id": trusted_envelope.event_id,
            "correlation_id": trusted_envelope.correlation_id,
            "payload": payload,
            "updated_at": func.now(),
        }
        with self.connection() as conn:
            conn.execute(
                select(func.pg_advisory_xact_lock(evidence_window_lock_key(evidence_key)))
            )
            current = (
                conn.execute(
                    select(window_table)
                    .where(window_table.c.evidence_key == evidence_key)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if current is None:
                inserted = (
                    conn.execute(
                        pg_insert(window_table)
                        .values(evidence_key=evidence_key, **new_values)
                        .on_conflict_do_nothing(index_elements=[window_table.c.evidence_key])
                        .returning(window_table.c.event_id, window_table.c.correlation_id)
                    )
                    .mappings()
                    .first()
                )
                if inserted is None:
                    existing = (
                        conn.execute(
                            select(
                                window_table.c.event_id,
                                window_table.c.correlation_id,
                            ).where(window_table.c.evidence_key == evidence_key)
                        )
                        .mappings()
                        .one()
                    )
                    return {"duplicate": True, **dict(existing)}
                self.stage_event_envelope(conn, event_table, outbox_table, trusted_envelope)
                return {"duplicate": False, **dict(inserted)}

            if (
                str(current["event_id"]) != expected_event_id
                or str(current["correlation_id"]) != expected_correlation_id
            ):
                return {
                    "duplicate": True,
                    "event_id": str(current["event_id"]),
                    "correlation_id": str(current["correlation_id"]),
                }

            history_digest = hashlib.sha256(
                f"{expected_event_id}\0{expected_correlation_id}".encode()
            ).hexdigest()[:24]
            history_key = f"{evidence_key}:history:{history_digest}"
            history_values = {
                key: current[key]
                for key in (
                    "workspace_id",
                    "cluster_id",
                    "source_id",
                    "window_start",
                    "agent_id",
                    "event_id",
                    "correlation_id",
                    "payload",
                    "created_at",
                    "updated_at",
                )
            }
            conn.execute(
                pg_insert(window_table)
                .values(evidence_key=history_key, **history_values)
                .on_conflict_do_nothing(index_elements=[window_table.c.evidence_key])
            )
            conn.execute(
                update(window_table)
                .where(
                    window_table.c.evidence_key == evidence_key,
                    window_table.c.event_id == expected_event_id,
                    window_table.c.correlation_id == expected_correlation_id,
                )
                .values(**new_values)
            )
            self.stage_event_envelope(conn, event_table, outbox_table, trusted_envelope)
        return {
            "duplicate": False,
            "event_id": trusted_envelope.event_id,
            "correlation_id": trusted_envelope.correlation_id,
        }

    def stage_event_envelope(
        self,
        conn: Any,
        event_table: Any,
        outbox_table: Any,
        event_envelope: EventEnvelope,
    ) -> None:
        conn.execute(
            pg_insert(event_table)
            .values(
                event_id=event_envelope.event_id,
                subject=event_envelope.subject,
                source=event_envelope.source,
                correlation_id=event_envelope.correlation_id,
                causation_id=event_envelope.causation_id,
                payload=event_envelope.payload,
            )
            .on_conflict_do_nothing(index_elements=[event_table.c.event_id])
        )
        conn.execute(
            pg_insert(outbox_table)
            .values(
                event_id=event_envelope.event_id,
                subject=event_envelope.subject,
                source=event_envelope.source,
                correlation_id=event_envelope.correlation_id,
                causation_id=event_envelope.causation_id,
                workspace_id=event_envelope.workspace_id,
                occurred_at=event_envelope.created_at,
                payload=event_envelope.payload,
                lease_id=None,
                leased_until=None,
            )
            .on_conflict_do_nothing(index_elements=[outbox_table.c.event_id])
        )

    def release_stale_pending_evidence_window(
        self,
        evidence_key: str,
        stale_after_seconds: int = DEFAULT_PENDING_EVIDENCE_EVENT_TTL_SECONDS,
    ) -> bool:
        table = EvidenceWindow.__table__
        stale_before = datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
        statement = (
            table.delete()
            .where(
                table.c.evidence_key == evidence_key,
                table.c.event_id.like(f"{PENDING_EVIDENCE_EVENT_ID_PREFIX}%"),
                table.c.updated_at < stale_before,
            )
            .returning(table.c.evidence_key)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return row is not None
