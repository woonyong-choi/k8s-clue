"""gitops 도메인 repository(SQL)."""

from __future__ import annotations

import hashlib
from collections.abc import Collection, Mapping, Sequence
from contextlib import nullcontext
from typing import Any

from sqlalchemy import and_, bindparam, case, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from domains.command.models import AgentCommand
from domains.gitops.diffing import snapshot_from_kubernetes_object
from domains.gitops.models import (
    Application,
    Approval,
    ApprovedResourceSnapshot,
    DeploymentBinding,
    GitRepository,
    GitSourceObservation,
    GitWatchTarget,
    ManifestArtifact,
    RepoChange,
    WorkflowRun,
    WorkflowRunStep,
    WorkspaceCredential,
)
from domains.gitops.repository_discovery import (
    RepositoryDiscoveryError,
    normalize_github_repo_ref,
)
from packages.config.constants import CommandStatus, Target
from packages.config.logs import get_logger
from packages.contracts.event_bus.interfaces import JsonObject
from packages.contracts.gitops import (
    DEFAULT_APPLICATION_ID,
    DEFAULT_DEPLOYMENT_BINDING_ID,
    DEFAULT_ENVIRONMENT,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_REPO_BRANCH,
    DEFAULT_REPO_REF,
    DEFAULT_REPOSITORY_ID,
    DEFAULT_WATCH_TARGET_ID,
    DEFAULT_WORKFLOW_RUN_ID,
    ApplicationStatus,
    ApprovalStatus,
    DeploymentBindingStatus,
    GitProvider,
    ManifestArtifactStatus,
    RepositoryStatus,
    ResourceClass,
    WatchTargetStatus,
    WorkflowMutation,
    WorkflowRunStatus,
    WorkflowStepName,
    WorkflowStepStatus,
    promotion_gate_from_command_result,
)
from packages.contracts.identity import (
    DEFAULT_WORKSPACE_ID,
    AccessResourceType,
    Permission,
    ResourceRole,
)
from packages.storage.engine import DatabaseConnection, iso_or_none, row_dict

LOGGER = get_logger(__name__)
GITHUB_REPOSITORY_CREDENTIAL_SCOPE_PREFIX = "repository"

# 원자 해결 대상으로 열림으로 간주하는 승인 상태 — 라우터의 open 판정과 동일해야 함
OPEN_APPROVAL_STATUSES = (
    ApprovalStatus.REQUESTED.value,
    ApprovalStatus.NOT_REQUIRED.value,
)

# 워크플로 상태 전이 순위 — 숫자가 클수록 뒤 단계. 같은 순위 재기록은 허용(멱등 재갱신).
# 재배달·지연 이벤트가 뒤 단계 상태를 앞 단계로 되돌리는 회귀(SUCCEEDED→APPLYING 등) 차단 기준.
WORKFLOW_STATUS_RANKS: dict[str, int] = {
    WorkflowRunStatus.STARTED.value: 1,
    WorkflowRunStatus.RENDERING.value: 2,
    WorkflowRunStatus.DIFFING.value: 3,
    WorkflowRunStatus.POLICY_CHECKING.value: 4,
    WorkflowRunStatus.WAITING_FOR_APPROVAL.value: 5,
    WorkflowRunStatus.APPLYING.value: 6,
    WorkflowRunStatus.ROLLOUT_WAITING.value: 7,
    WorkflowRunStatus.SUCCEEDED.value: 8,
    WorkflowRunStatus.FAILED.value: 8,  # 실패는 어느 단계에서도 도달 가능 → 최고 순위
}
# 종결 상태 — 어떤 상태로도 다시 갱신되지 않음(회귀 불가)
TERMINAL_WORKFLOW_STATUSES = (
    WorkflowRunStatus.SUCCEEDED.value,
    WorkflowRunStatus.FAILED.value,
)

# 단계 상태도 같은 원칙으로 단조 증가한다. 종결 상태는 늦은 재배달로 덮지 않는다.
WORKFLOW_STEP_STATUS_RANKS: dict[str, int] = {
    WorkflowStepStatus.PENDING.value: 1,
    WorkflowStepStatus.RUNNING.value: 2,
    WorkflowStepStatus.SUCCEEDED.value: 3,
    WorkflowStepStatus.FAILED.value: 3,
    WorkflowStepStatus.SKIPPED.value: 3,
}
TERMINAL_WORKFLOW_STEP_STATUSES = (
    WorkflowStepStatus.SUCCEEDED.value,
    WorkflowStepStatus.FAILED.value,
    WorkflowStepStatus.SKIPPED.value,
)

WORKFLOW_MUTATION_FIELDS = (
    "workflow_run_id",
    "workspace_id",
    "application_id",
    "binding_id",
    "environment",
    "cluster_id",
    "commit_sha",
    "status",
    "current_step",
)
WORKFLOW_STEP_MUTATION_FIELDS = (
    "workflow_run_id",
    "workspace_id",
    "application_id",
    "binding_id",
    "environment",
    "name",
    "status",
)


def workflow_status_rank(column: Any) -> Any:
    """상태 컬럼을 전이 순위로 바꾸는 CASE 식 — guarded UPDATE 의 비교 기준."""
    return case(
        *[(column == status, rank) for status, rank in WORKFLOW_STATUS_RANKS.items()],
        else_=0,
    )


def workflow_transition_guard(table: Any, new_status: Any) -> Any:
    """허용 전이 조건: 현재가 종결이 아니고 새 상태 순위가 현재 순위 이상임.

    new_status 는 문자열(guarded UPDATE) 또는 excluded 컬럼(upsert) 모두 가능.
    """
    new_rank = (
        WORKFLOW_STATUS_RANKS.get(str(new_status), 0)
        if isinstance(new_status, str)
        else workflow_status_rank(new_status)
    )
    return and_(
        table.c.status.not_in(TERMINAL_WORKFLOW_STATUSES),
        workflow_status_rank(table.c.status) <= new_rank,
    )


def workflow_step_status_rank(column: Any) -> Any:
    """단계 상태 컬럼을 전이 순위로 바꾸는 CASE 식."""
    return case(
        *[(column == status, rank) for status, rank in WORKFLOW_STEP_STATUS_RANKS.items()],
        else_=0,
    )


def workflow_step_transition_guard(table: Any, new_status: Any) -> Any:
    """종결 단계 고정과 pending/running 역행 방지를 한 SQL 조건으로 보장한다."""
    new_rank = (
        WORKFLOW_STEP_STATUS_RANKS.get(str(new_status), 0)
        if isinstance(new_status, str)
        else workflow_step_status_rank(new_status)
    )
    return and_(
        table.c.status.not_in(TERMINAL_WORKFLOW_STEP_STATUSES),
        workflow_step_status_rank(table.c.status) <= new_rank,
    )


def workflow_mutation_from_result(
    result: object,
    *,
    fields: tuple[str, ...],
) -> WorkflowMutation:
    """Return a safe identity only when PostgreSQL actually changed a row.

    Guarded ``UPDATE`` and ``ON CONFLICT ... WHERE`` return no row for a
    rejected or replayed state.  Callers use this exact outcome to suppress
    outbox bodies and Timeline facts rather than inferring success from input.
    """
    mappings = getattr(result, "mappings", None)
    if not callable(mappings):
        return WorkflowMutation(applied=False)
    row = mappings().one_or_none()
    if not isinstance(row, Mapping):
        return WorkflowMutation(applied=False)
    return WorkflowMutation(
        applied=True,
        values={field: str(row[field]) for field in fields if row.get(field) is not None},
    )


def watch_target_settings(payload: JsonObject) -> JsonObject:
    settings = dict(payload.get("settings", {}))
    deploy_policy = dict(payload.get("deploy_policy", {}))
    source_type = str(
        settings.get("source_type")
        or deploy_policy.get("manifest_source")
        or deploy_policy.get("source_type")
        or ""
    ).strip()
    if source_type:
        settings["source_type"] = source_type
    return settings


def prepare_approved_resource_snapshots(
    workflow_run_id: str,
    run_row: Mapping[str, object],
    command_rows: Sequence[Mapping[str, object]],
) -> tuple[int, list[JsonObject]] | None:
    """Validate every applied command and prepare snapshots for supported kinds.

    ``snapshot_from_kubernetes_object`` intentionally returns no managed fields
    for kinds outside the current diff model (for example RBAC, PDB and Job).
    Those commands are still part of a successfully applied workflow, so they
    count as handled but do not create an empty snapshot row.
    """

    prepared: list[JsonObject] = []
    for row in command_rows:
        payload = row["payload"] if isinstance(row.get("payload"), Mapping) else {}
        diff = payload.get("diff")
        if not isinstance(diff, Mapping):
            return None
        desired_manifest = diff.get("desired_manifest")
        if not isinstance(desired_manifest, Mapping) or not desired_manifest:
            return None
        desired = snapshot_from_kubernetes_object(
            desired_manifest,
            source="approved_command",
        )
        expected_resource = str(diff.get("resource") or "").casefold()
        expected_namespace = str(diff.get("namespace") or "")
        if (
            desired.resource.casefold() != expected_resource
            or desired.namespace != expected_namespace
            or str(diff.get("workflow_run_id") or "") != workflow_run_id
            or str(diff.get("workspace_id") or "") != str(run_row["workspace_id"])
            or str(diff.get("binding_id") or "") != str(run_row["binding_id"])
            or str(diff.get("cluster_id") or "") != str(run_row["cluster_id"])
        ):
            return None
        if not desired.fields:
            continue
        kind, name = desired.resource.split("/", 1)
        basis = diff.get("basis")
        artifact_digest = (
            str(basis.get("artifact_digest") or "") if isinstance(basis, Mapping) else ""
        )
        prepared.append(
            {
                "workspace_id": str(run_row["workspace_id"]),
                "binding_id": str(run_row["binding_id"]),
                "cluster_id": str(run_row["cluster_id"]),
                "namespace": desired.namespace,
                "resource_kind": kind,
                "resource_name": name,
                "workflow_run_id": workflow_run_id,
                "command_id": str(row["command_id"]),
                "commit_sha": str(run_row["commit_sha"]),
                "artifact_digest": artifact_digest,
                "managed_fields": {"paths": sorted(desired.fields)},
                "snapshot": {
                    "resource": desired.resource,
                    "namespace": desired.namespace,
                    "fields": desired.fields,
                },
                "completed_at": row.get("completed_at") or func.now(),
                "updated_at": func.now(),
            }
        )
    return len(command_rows), prepared


class RepoChangeRepository(DatabaseConnection):
    def lock_repository_identity(self, workspace_id: str, repo_ref: str) -> None:
        lock_key = repository_identity_lock_key(workspace_id, repo_ref)
        with self.connection() as conn:
            conn.execute(select(func.pg_advisory_xact_lock(lock_key)))

    def lock_workspace_credential_scope(
        self,
        workspace_id: str,
        provider: str,
        scope: str,
        *,
        conn: Any | None = None,
    ) -> None:
        lock_key = workspace_credential_lock_key(workspace_id, provider, scope)
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            connection.execute(select(func.pg_advisory_xact_lock(lock_key)))

    def upsert_workspace_credential(
        self,
        payload: JsonObject,
        *,
        conn: Any | None = None,
    ) -> JsonObject:
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        provider = str(payload["provider"])
        scope = str(payload["scope"])
        credential_id = str(
            payload.get("credential_id") or stable_credential_id(workspace_id, provider, scope)
        )
        table = WorkspaceCredential.__table__
        insert = pg_insert(table).values(
            credential_id=credential_id,
            workspace_id=workspace_id,
            provider=provider,
            scope=scope,
            encrypted_value=str(payload["encrypted_value"]),
            status=str(payload.get("status", "active")),
            metadata=dict(payload.get("metadata", {})),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.workspace_id, table.c.provider, table.c.scope],
            set_={
                "encrypted_value": insert.excluded.encrypted_value,
                "status": insert.excluded.status,
                "metadata": insert.excluded.metadata,
                "updated_at": func.now(),
            },
        ).returning(table)
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            row = connection.execute(statement).mappings().one()
        return row_dict(row)

    def get_workspace_credential(
        self,
        workspace_id: str,
        provider: str,
        scope: str,
        *,
        conn: Any | None = None,
    ) -> JsonObject | None:
        table = WorkspaceCredential.__table__
        statement = select(table).where(
            table.c.workspace_id == workspace_id,
            table.c.provider == provider,
            table.c.scope == scope,
            table.c.status == "active",
        )
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            row = connection.execute(statement).mappings().first()
        return row_dict(row) if row is not None else None

    def update_workspace_credential_metadata(
        self,
        *,
        workspace_id: str,
        provider: str,
        scope: str,
        expected_revision: str,
        metadata: JsonObject,
        expected_state: str | None = None,
        conn: Any | None = None,
    ) -> JsonObject | None:
        """Merge non-secret probe evidence only for the exact active revision."""

        if not all((workspace_id, provider, scope, expected_revision)):
            return None
        table = WorkspaceCredential.__table__
        statement = (
            table.update()
            .where(
                table.c.workspace_id == workspace_id,
                table.c.provider == provider,
                table.c.scope == scope,
                table.c.status == "active",
                table.c.metadata["revision"].astext == expected_revision,
            )
            .values(
                metadata=table.c.metadata.concat(dict(metadata)),
                updated_at=func.now(),
            )
            .returning(table)
        )
        if expected_state is not None:
            statement = statement.where(table.c.metadata["state"].astext == expected_state)
        context = nullcontext(conn) if conn is not None else self.connection()
        with context as connection:
            row = connection.execute(statement).mappings().first()
        return row_dict(row) if row is not None else None

    def delete_workspace_credential(self, workspace_id: str, provider: str, scope: str) -> bool:
        """Delete one exact workspace credential scope without reading its secret value."""

        if not workspace_id or not provider or not scope:
            return False
        table = WorkspaceCredential.__table__
        statement = (
            table.delete()
            .where(
                table.c.workspace_id == workspace_id,
                table.c.provider == provider,
                table.c.scope == scope,
            )
            .returning(table.c.credential_id)
        )
        with self.connection() as conn:
            return conn.execute(statement).scalar_one_or_none() is not None

    def get_repository_by_ref(self, workspace_id: str, repo_ref: str) -> JsonObject | None:
        if not workspace_id or not repo_ref:
            return None
        try:
            canonical_repo_ref = normalize_github_repo_ref(repo_ref)
        except (RepositoryDiscoveryError, ValueError):
            return None
        table = GitRepository.__table__
        exact_statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repo_ref == canonical_repo_ref,
            )
            .limit(1)
        )
        legacy_statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                func.lower(table.c.repo_ref) == canonical_repo_ref,
            )
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(exact_statement).mappings().first()
            if row is None:
                row = conn.execute(legacy_statement).mappings().first()
        return row_dict(row) if row is not None else None

    def list_repository_applications(
        self,
        workspace_id: str,
        repository_id: str,
    ) -> list[JsonObject]:
        table = Application.__table__
        statement = (
            select(table.c.application_id)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repository_id == repository_id,
            )
            .order_by(table.c.application_id)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def can_manage_repository(
        self,
        user_id: str,
        workspace_id: str,
        repository_id: str,
    ) -> bool:
        is_service_admin = getattr(self, "is_service_admin", None)
        if callable(is_service_admin) and is_service_admin(user_id):
            return True
        applications = self.list_repository_applications(workspace_id, repository_id)
        if not applications:
            return False
        can_access = getattr(self, "can_access", None)
        if not callable(can_access):
            return False
        decisions = [
            bool(
                can_access(
                    user_id,
                    workspace_id,
                    AccessResourceType.APPLICATION.value,
                    str(application["application_id"]),
                    Permission.APPLICATION_MANAGE.value,
                )
            )
            for application in applications
        ]
        return all(decisions)

    def register_repository(self, payload: JsonObject) -> JsonObject:
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        payload = {
            **payload,
            "repo_ref": normalize_github_repo_ref(str(payload.get("repo_ref", DEFAULT_REPO_REF))),
        }
        # Create IDs are always server-derived.  ``derive_repository_id`` must
        # keep accepting explicit IDs for event/worker identity normalization,
        # so remove the untrusted create field only at this storage boundary.
        create_identity = {**payload}
        create_identity.pop("repository_id", None)
        derived_repository_id = derive_repository_id(create_identity)
        user_id = payload.get("user_id")
        table = GitRepository.__table__
        with self.unit_of_work():
            if user_id:
                self.lock_repository_identity(workspace_id, str(payload["repo_ref"]))
            existing = self.get_repository_by_ref(
                workspace_id,
                str(payload.get("repo_ref", DEFAULT_REPO_REF)),
            )
            if existing is not None:
                repository_id = str(existing["repository_id"])
                if not user_id or not self.can_manage_repository(
                    str(user_id),
                    workspace_id,
                    repository_id,
                ):
                    raise LookupError("repository not found in workspace")
                authorized_repository_id: str | None = repository_id
            else:
                repository_id = derived_repository_id
                # A row appearing after the authoritative lookup must not be
                # updated by this create attempt. The caller can retry and go
                # through the existing-row manage check.
                authorized_repository_id = None

            insert = pg_insert(table).values(
                repository_id=repository_id,
                workspace_id=workspace_id,
                provider=str(payload.get("provider", GitProvider.GITHUB.value)),
                repo_ref=str(payload.get("repo_ref", DEFAULT_REPO_REF)),
                default_branch=str(payload.get("default_branch", DEFAULT_REPO_BRANCH)),
                credential_ref=payload.get("credential_ref"),
                status=str(payload.get("status", RepositoryStatus.ACTIVE.value)),
                access_policy=dict(payload.get("access_policy", {})),
                updated_at=func.now(),
            )
            statement = insert.on_conflict_do_update(
                index_elements=[table.c.repository_id],
                set_={
                    "provider": insert.excluded.provider,
                    "repo_ref": insert.excluded.repo_ref,
                    "default_branch": insert.excluded.default_branch,
                    "credential_ref": (
                        insert.excluded.credential_ref
                        if "credential_ref" in payload
                        else table.c.credential_ref
                    ),
                    "status": insert.excluded.status,
                    "access_policy": insert.excluded.access_policy,
                    "updated_at": func.now(),
                },
                where=and_(
                    table.c.workspace_id == insert.excluded.workspace_id,
                    table.c.repository_id
                    == bindparam("authorized_repository_id", authorized_repository_id),
                ),
            ).returning(table)
            with self.connection() as conn:
                row = conn.execute(statement).mappings().first()
            if row is None:
                # Conditional conflict updates deliberately return no row for
                # foreign ownership, unapproved races, or stale identities.
                raise LookupError("repository not found in workspace")
        return {
            **payload,
            **row_dict(row),
            "workspace_id": workspace_id,
            "repository_id": repository_id,
        }

    def _cascade_repository_status(
        self,
        conn: Any,
        *,
        workspace_id: str,
        repository_id: str,
        repository_status: str,
        pause_children: bool,
    ) -> dict[str, int]:
        """Repository 상태를 옮기고, 필요하면 자식(watch/binding/application)까지
        비활성으로 함께 내려 고아 상태를 남기지 않는다.

        active 조인들은 모두 ``status == ACTIVE`` 만 보므로, 자식을 비활성으로
        내리면 폴링·동기화·목록에서 일관되게 사라진다(부분적으로만 남는 모순 방지).
        반환은 각 테이블에서 실제로 바뀐 행 수(관측/검증용).
        """
        repo_table = GitRepository.__table__
        conn.execute(
            repo_table.update()
            .where(
                repo_table.c.workspace_id == workspace_id,
                repo_table.c.repository_id == repository_id,
            )
            .values(status=repository_status, updated_at=func.now())
        )
        changed = {"repository": 1, "watch_targets": 0, "bindings": 0, "applications": 0}
        if not pause_children:
            return changed
        watch_table = GitWatchTarget.__table__
        watch_result = conn.execute(
            watch_table.update()
            .where(
                watch_table.c.workspace_id == workspace_id,
                watch_table.c.repository_id == repository_id,
                watch_table.c.status == WatchTargetStatus.ACTIVE.value,
            )
            .values(status=WatchTargetStatus.PAUSED.value, updated_at=func.now())
        )
        binding_table = DeploymentBinding.__table__
        binding_result = conn.execute(
            binding_table.update()
            .where(
                binding_table.c.workspace_id == workspace_id,
                binding_table.c.repository_id == repository_id,
                binding_table.c.status == DeploymentBindingStatus.ACTIVE.value,
            )
            .values(status=DeploymentBindingStatus.PAUSED.value, updated_at=func.now())
        )
        app_table = Application.__table__
        app_result = conn.execute(
            app_table.update()
            .where(
                app_table.c.workspace_id == workspace_id,
                app_table.c.repository_id == repository_id,
                app_table.c.status == ApplicationStatus.ACTIVE.value,
            )
            .values(status=ApplicationStatus.ARCHIVED.value, updated_at=func.now())
        )
        changed["watch_targets"] = int(watch_result.rowcount or 0)
        changed["bindings"] = int(binding_result.rowcount or 0)
        changed["applications"] = int(app_result.rowcount or 0)
        return changed

    def set_repository_connection_status(
        self,
        workspace_id: str,
        repo_ref: str,
        status: str,
        *,
        pause_children: bool = True,
    ) -> JsonObject | None:
        """외부(GitHub) 변경 감지 등으로 저장소 연결 상태를 옮긴다(웹훅/폴러 공용).

        저장소가 없으면 ``None``(멱등 무동작). 존재하면 상태를 옮기고, 폴링이 계속
        실패·오작동하지 않게 자식까지 함께 정지시켜 부분 활성 모순을 막는다.
        """
        with self.unit_of_work():
            existing = self.get_repository_by_ref(workspace_id, repo_ref)
            if existing is None:
                return None
            repository_id = str(existing["repository_id"])
            with self.connection() as conn:
                changed = self._cascade_repository_status(
                    conn,
                    workspace_id=workspace_id,
                    repository_id=repository_id,
                    repository_status=status,
                    pause_children=pause_children,
                )
            refreshed = self.get_repository_by_ref(workspace_id, repo_ref) or existing
        return {**refreshed, "cascade": changed}

    def disconnect_repository(
        self,
        workspace_id: str,
        repo_ref: str,
        *,
        drop_credential: bool = True,
    ) -> JsonObject | None:
        """사용자 요청으로 저장소 연결을 해제한다(종단 상태 + 자식 정지 + 자격 삭제).

        고아 방지 계약:
          - repository → ``disconnected``
          - watch_targets → ``paused`` (폴링 중단)
          - deployment_bindings → ``paused`` (동기화 중단)
          - applications → ``archived`` (목록에서 제외)
          - 저장된 repo-scope PAT 자격증명 삭제(App 설치 참조는 vault 밖이라 무동작)
        저장소가 없으면 ``None``(멱등). 이미 해제됨이면 다시 안전하게 수렴한다.
        """
        with self.unit_of_work():
            existing = self.get_repository_by_ref(workspace_id, repo_ref)
            if existing is None:
                return None
            repository_id = str(existing["repository_id"])
            self.lock_workspace_credential_scope(
                workspace_id, "github", repository_credential_scope(repository_id)
            )
            with self.connection() as conn:
                changed = self._cascade_repository_status(
                    conn,
                    workspace_id=workspace_id,
                    repository_id=repository_id,
                    repository_status=RepositoryStatus.DISCONNECTED.value,
                    pause_children=True,
                )
            credential_dropped = False
            if drop_credential:
                credential_dropped = self.delete_workspace_credential(
                    workspace_id, "github", repository_credential_scope(repository_id)
                )
            refreshed = self.get_repository_by_ref(workspace_id, repo_ref) or existing
        return {
            **refreshed,
            "cascade": changed,
            "credential_dropped": credential_dropped,
        }

    def list_repositories_by_credential_ref(
        self,
        workspace_id: str,
        credential_ref: str,
    ) -> list[JsonObject]:
        """특정 자격증명 참조(예: App 설치 참조)에 묶인 활성/비활성 저장소 목록.

        installation 삭제·권한 회수 웹훅에서 영향받는 저장소를 찾아 상태를 내릴 때 쓴다.
        """
        if not workspace_id or not credential_ref:
            return []
        table = GitRepository.__table__
        statement = select(table).where(
            table.c.workspace_id == workspace_id,
            table.c.credential_ref == credential_ref,
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [row_dict(row) for row in rows]

    def list_repositories(self, workspace_id: str) -> list[JsonObject]:
        """워크스페이스의 모든 저장소를 상태와 활성 앱 수와 함께 나열한다.

        활성 뷰(active 조인)와 달리 degraded/disconnected 저장소도 포함해, 연결 상태
        관리 화면이 '연결은 있는데 상태가 나쁜' 것까지 보여줄 수 있게 한다.
        """
        if not workspace_id:
            return []
        repo = GitRepository.__table__
        app = Application.__table__
        app_count = (
            select(
                app.c.repository_id.label("repository_id"),
                func.count().label("application_count"),
            )
            .where(
                app.c.workspace_id == workspace_id,
                app.c.status == ApplicationStatus.ACTIVE.value,
            )
            .group_by(app.c.repository_id)
            .subquery()
        )
        statement = (
            select(
                repo,
                func.coalesce(app_count.c.application_count, 0).label("application_count"),
            )
            .select_from(
                repo.outerjoin(app_count, repo.c.repository_id == app_count.c.repository_id)
            )
            .where(repo.c.workspace_id == workspace_id)
            .order_by(repo.c.repo_ref.asc())
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [row_dict(row) for row in rows]

    def register_watch_target(self, payload: JsonObject) -> JsonObject:
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        repository_id = derive_repository_id(payload)
        watch_target_id = derive_watch_target_id({**payload, "repository_id": repository_id})
        table = GitWatchTarget.__table__
        insert = pg_insert(table).values(
            watch_target_id=watch_target_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            branch=str(payload.get("branch", DEFAULT_REPO_BRANCH)),
            manifest_path=str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
            interval_seconds=int(payload.get("interval_seconds", 30)),
            last_seen_commit_sha=payload.get("last_seen_commit_sha"),
            last_polled_at=payload.get("last_polled_at"),
            status=str(payload.get("status", WatchTargetStatus.ACTIVE.value)),
            settings=watch_target_settings(payload),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.watch_target_id],
            set_={
                "branch": insert.excluded.branch,
                "manifest_path": insert.excluded.manifest_path,
                "interval_seconds": insert.excluded.interval_seconds,
                "last_seen_commit_sha": insert.excluded.last_seen_commit_sha,
                "last_polled_at": insert.excluded.last_polled_at,
                "status": insert.excluded.status,
                "settings": insert.excluded.settings,
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)
        return {
            **payload,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
            "watch_target_id": watch_target_id,
        }

    def register_deployment_binding(self, payload: JsonObject) -> JsonObject:
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        repository_id = derive_repository_id(payload)
        watch_target_id = derive_watch_target_id({**payload, "repository_id": repository_id})
        binding_id = derive_deployment_binding_id(
            {**payload, "repository_id": repository_id, "watch_target_id": watch_target_id}
        )
        user_id = payload.get("user_id")
        table = DeploymentBinding.__table__
        insert = pg_insert(table).values(
            binding_id=binding_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            watch_target_id=watch_target_id,
            cluster_id=str(payload["cluster_id"]),
            namespace=str(payload["namespace"]),
            app_name=str(payload["app_name"]),
            manifest_path=str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
            environment=str(payload.get("environment", "sandbox")),
            resource_class=str(payload.get("resource_class", ResourceClass.APPLICATION.value)),
            status=str(payload.get("status", DeploymentBindingStatus.ACTIVE.value)),
            deploy_policy=dict(payload.get("deploy_policy", {})),
            access_policy=dict(payload.get("access_policy", {})),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.binding_id],
            set_={
                "watch_target_id": insert.excluded.watch_target_id,
                "cluster_id": insert.excluded.cluster_id,
                "namespace": insert.excluded.namespace,
                "app_name": insert.excluded.app_name,
                "manifest_path": insert.excluded.manifest_path,
                "environment": insert.excluded.environment,
                "resource_class": insert.excluded.resource_class,
                "status": insert.excluded.status,
                "deploy_policy": insert.excluded.deploy_policy,
                "access_policy": insert.excluded.access_policy,
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)
        self._grant_owner_if_present(
            workspace_id, user_id, AccessResourceType.DEPLOYMENT_BINDING.value, binding_id
        )
        return {
            **payload,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
            "watch_target_id": watch_target_id,
            "binding_id": binding_id,
        }

    def upsert_application(self, payload: JsonObject) -> JsonObject:
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        repository_id = derive_repository_id(payload)
        application_id = derive_application_id(payload)
        name = derive_application_name(payload) or DEFAULT_APPLICATION_ID
        table = Application.__table__
        existing_application_id_statement = (
            select(table.c.application_id)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repository_id == repository_id,
                table.c.name == name,
            )
            .limit(1)
            .with_for_update()
        )
        update_values = {
            "repository_id": repository_id,
            "name": name,
            "manifest_path": str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
            "status": str(payload.get("status", ApplicationStatus.ACTIVE.value)),
            "metadata": dict(payload.get("metadata", {})),
            "updated_at": func.now(),
        }
        create_statement = (
            pg_insert(table)
            .values(
                application_id=application_id,
                workspace_id=workspace_id,
                repository_id=repository_id,
                name=name,
                manifest_path=str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
                status=str(payload.get("status", ApplicationStatus.ACTIVE.value)),
                metadata=dict(payload.get("metadata", {})),
                updated_at=func.now(),
            )
            .on_conflict_do_nothing()
            .returning(table.c.application_id)
        )
        user_id = str(payload.get("user_id") or "")
        created = False
        with self.unit_of_work():
            repo_ref = str(payload.get("repo_ref") or "").strip()
            if repo_ref:
                self.lock_repository_identity(workspace_id, repo_ref)
            if user_id:
                self.lock_application_identity(workspace_id, repository_id, name)
            with self.connection() as conn:
                existing_application_id = conn.execute(
                    existing_application_id_statement
                ).scalar_one_or_none()
            with self.connection() as conn:
                if existing_application_id is None:
                    inserted_application_id = conn.execute(create_statement).scalar_one_or_none()
                    if inserted_application_id is not None:
                        resolved_application_id = str(inserted_application_id)
                        created = True
                    else:
                        existing_application_id = conn.execute(
                            existing_application_id_statement
                        ).scalar_one_or_none()

                if not created:
                    if existing_application_id is None:
                        # A conflicting primary key that is not the same
                        # workspace/repository/name identity is never adopted.
                        raise LookupError("application not found in workspace")
                    resolved_application_id = str(existing_application_id)
                    if user_id:
                        can_access = getattr(self, "can_access", None)
                        if not callable(can_access) or not can_access(
                            user_id,
                            workspace_id,
                            AccessResourceType.APPLICATION.value,
                            resolved_application_id,
                            Permission.APPLICATION_MANAGE.value,
                        ):
                            raise LookupError("application not found in workspace")
                    if resolved_application_id != application_id:
                        LOGGER.warning(
                            "application_id_merged_by_name",
                            extra={
                                "context": {
                                    "workspace_id": workspace_id,
                                    "repository_id": repository_id,
                                    "name": name,
                                    "incoming_application_id": application_id,
                                    "resolved_application_id": resolved_application_id,
                                }
                            },
                        )
                    updated_application_id = conn.execute(
                        table.update()
                        .where(
                            table.c.workspace_id == workspace_id,
                            table.c.repository_id == repository_id,
                            table.c.name == name,
                            table.c.application_id == resolved_application_id,
                        )
                        .values(**update_values)
                        .returning(table.c.application_id)
                    ).scalar_one_or_none()
                    if updated_application_id is None:
                        raise LookupError("application not found in workspace")
                    resolved_application_id = str(updated_application_id)
        if created:
            self._grant_owner_if_present(
                workspace_id,
                payload.get("user_id"),
                AccessResourceType.APPLICATION.value,
                resolved_application_id,
            )
        return {**payload, "workspace_id": workspace_id, "application_id": resolved_application_id}

    def lock_application_identity(
        self,
        workspace_id: str,
        repository_id: str,
        name: str,
    ) -> None:
        """Serialize user-originated create/update authorization for one app identity."""
        lock_key = application_identity_lock_key(workspace_id, repository_id, name)
        with self.connection() as conn:
            conn.execute(select(func.pg_advisory_xact_lock(lock_key)))

    def list_applications(
        self,
        workspace_id: str,
        *,
        application_ids: set[str] | None = None,
        limit: int = 100,
    ) -> list[JsonObject]:
        if application_ids is not None and not application_ids:
            return []
        table = Application.__table__
        repo_table = GitRepository.__table__
        statement = (
            select(
                table.c.application_id,
                table.c.workspace_id,
                table.c.repository_id,
                table.c.name,
                table.c.manifest_path,
                table.c.status,
                table.c.metadata,
                repo_table.c.repo_ref,
                repo_table.c.default_branch,
                table.c.created_at,
                table.c.updated_at,
            )
            .join(
                repo_table,
                and_(
                    repo_table.c.workspace_id == table.c.workspace_id,
                    repo_table.c.repository_id == table.c.repository_id,
                ),
            )
            .where(table.c.workspace_id == workspace_id)
            .order_by(table.c.name, table.c.application_id)
            .limit(max(1, min(limit, 500)))
        )
        if application_ids is not None:
            statement = statement.where(table.c.application_id.in_(application_ids))
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [serialize_application(row) for row in rows]

    def get_application(self, workspace_id: str, application_id: str) -> JsonObject | None:
        table = Application.__table__
        repo_table = GitRepository.__table__
        statement = (
            select(
                table.c.application_id,
                table.c.workspace_id,
                table.c.repository_id,
                table.c.name,
                table.c.manifest_path,
                table.c.status,
                table.c.metadata,
                repo_table.c.repo_ref,
                repo_table.c.default_branch,
                table.c.created_at,
                table.c.updated_at,
            )
            .join(
                repo_table,
                and_(
                    repo_table.c.workspace_id == table.c.workspace_id,
                    repo_table.c.repository_id == table.c.repository_id,
                ),
            )
            .where(table.c.workspace_id == workspace_id, table.c.application_id == application_id)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return serialize_application(row) if row else None

    def get_application_by_identity(
        self,
        workspace_id: str,
        repository_id: str,
        name: str,
    ) -> JsonObject | None:
        table = Application.__table__
        statement = (
            select(table.c.application_id)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repository_id == repository_id,
                table.c.name == name,
            )
            .limit(1)
        )
        with self.connection() as conn:
            application_id = conn.execute(statement).scalar_one_or_none()
        if application_id is None:
            return None
        return self.get_application(workspace_id, str(application_id))

    def list_application_deployment_bindings(
        self,
        workspace_id: str,
        application_id: str,
        *,
        limit: int = 100,
    ) -> list[JsonObject]:
        application = self.get_application(workspace_id, application_id)
        if application is None:
            return []
        table = DeploymentBinding.__table__
        binding_identity = [table.c.app_name == application["name"]]
        manifest_path = str(application.get("manifest_path") or "").strip()
        if manifest_path:
            binding_identity.append(table.c.manifest_path == manifest_path)
        watch_table = GitWatchTarget.__table__
        watch_by_id = watch_table.alias("binding_watch_by_id")
        watch_by_source = watch_table.alias("binding_watch_by_source")
        default_branch = str(application.get("default_branch") or DEFAULT_REPO_BRANCH)
        statement = (
            select(
                table.c.binding_id,
                table.c.workspace_id,
                table.c.repository_id,
                table.c.watch_target_id,
                table.c.cluster_id,
                table.c.namespace,
                table.c.app_name,
                table.c.manifest_path,
                table.c.environment,
                table.c.resource_class,
                table.c.status,
                table.c.deploy_policy,
                table.c.access_policy,
                table.c.created_at,
                table.c.updated_at,
                func.coalesce(
                    watch_by_id.c.last_seen_commit_sha,
                    watch_by_source.c.last_seen_commit_sha,
                ).label("watch_last_seen_commit_sha"),
                func.coalesce(
                    watch_by_id.c.last_polled_at,
                    watch_by_source.c.last_polled_at,
                ).label("watch_last_polled_at"),
                func.coalesce(watch_by_id.c.settings, watch_by_source.c.settings).label(
                    "watch_settings"
                ),
            )
            .outerjoin(
                watch_by_id,
                and_(
                    watch_by_id.c.workspace_id == table.c.workspace_id,
                    watch_by_id.c.repository_id == table.c.repository_id,
                    watch_by_id.c.watch_target_id == table.c.watch_target_id,
                ),
            )
            .outerjoin(
                watch_by_source,
                and_(
                    watch_by_id.c.watch_target_id.is_(None),
                    watch_by_source.c.workspace_id == table.c.workspace_id,
                    watch_by_source.c.repository_id == table.c.repository_id,
                    watch_by_source.c.branch == default_branch,
                    watch_by_source.c.manifest_path == table.c.manifest_path,
                ),
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repository_id == application["repository_id"],
                or_(*binding_identity),
            )
            .order_by(table.c.environment, table.c.cluster_id, table.c.namespace)
            .limit(max(1, min(limit, 500)))
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [serialize_deployment_binding(row) for row in rows]

    def list_application_workflow_runs(
        self,
        workspace_id: str,
        application_id: str,
        *,
        limit: int = 100,
    ) -> list[JsonObject]:
        table = WorkflowRun.__table__
        statement = (
            select(
                table.c.workflow_run_id,
                table.c.workspace_id,
                table.c.application_id,
                table.c.binding_id,
                table.c.environment,
                table.c.cluster_id,
                table.c.commit_sha,
                table.c.status,
                table.c.current_step,
                table.c.summary,
                table.c.command_id,
                table.c.metadata,
                table.c.created_at,
                table.c.updated_at,
            )
            .where(table.c.workspace_id == workspace_id, table.c.application_id == application_id)
            .order_by(table.c.created_at.desc())
            .limit(max(1, min(limit, 500)))
        )
        step_table = WorkflowRunStep.__table__
        approval_table = Approval.__table__
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
            runs = [serialize_workflow_run(row) for row in rows]
            run_ids = [str(run["workflow_run_id"]) for run in runs]
            if run_ids:
                # 단계별 상세(details 에 diff plan 포함)를 함께 실어 콘솔이 리소스 단위
                # +/~/- 미리보기를 그릴 수 있게 함 — 별도 라운드트립 없이 한 응답으로.
                step_rows = (
                    conn.execute(
                        select(
                            step_table.c.workflow_run_id,
                            step_table.c.name,
                            step_table.c.status,
                            step_table.c.message,
                            step_table.c.details,
                            step_table.c.updated_at,
                        )
                        .where(step_table.c.workflow_run_id.in_(run_ids))
                        .order_by(step_table.c.created_at)
                    )
                    .mappings()
                    .all()
                )
                steps_by_run: dict[str, list[JsonObject]] = {}
                for step in step_rows:
                    steps_by_run.setdefault(str(step["workflow_run_id"]), []).append(
                        {
                            "name": step["name"],
                            "status": step["status"],
                            "message": step["message"],
                            "details": dict(step["details"] or {}),
                            "updated_at": iso_or_none(step["updated_at"]),
                        }
                    )
                for run in runs:
                    run["steps"] = steps_by_run.get(str(run["workflow_run_id"]), [])
                approval_rows = (
                    conn.execute(
                        select(
                            approval_table.c.approval_id,
                            approval_table.c.workflow_run_id,
                            approval_table.c.status,
                            approval_table.c.reason,
                            approval_table.c.requested_role,
                            approval_table.c.requested_by,
                            approval_table.c.decided_by,
                            approval_table.c.decision,
                            approval_table.c.details,
                            approval_table.c.updated_at,
                        )
                        .where(
                            approval_table.c.workspace_id == workspace_id,
                            approval_table.c.workflow_run_id.in_(run_ids),
                        )
                        .order_by(approval_table.c.updated_at.desc())
                    )
                    .mappings()
                    .all()
                )
                approvals_by_run: dict[str, list[JsonObject]] = {}
                for approval in approval_rows:
                    details = dict(approval["details"] or {})
                    approvals_by_run.setdefault(str(approval["workflow_run_id"]), []).append(
                        {
                            "approval_id": approval["approval_id"],
                            "status": approval["status"],
                            "reason": approval["reason"],
                            "requested_role": approval["requested_role"],
                            "requested_by": approval["requested_by"],
                            "decided_by": approval["decided_by"],
                            "decision": approval["decision"],
                            "details": details,
                            "updated_at": iso_or_none(approval["updated_at"]),
                        }
                    )
                for run in runs:
                    approvals = approvals_by_run.get(str(run["workflow_run_id"]), [])
                    run["approvals"] = approvals
                    current = current_workflow_approval(approvals)
                    if current is not None:
                        run["approval_id"] = current["approval_id"]
                        run["approval_status"] = current["status"]
                        run["approval_reason"] = current["reason"]
                        run["requested_role"] = current["requested_role"]
                        run["approval_details"] = current["details"]
                        run["approval_updated_at"] = current["updated_at"]
        return runs

    def get_deployment_binding(self, workspace_id: str, binding_id: str) -> JsonObject | None:
        table = DeploymentBinding.__table__
        statement = (
            select(table)
            .where(table.c.workspace_id == workspace_id, table.c.binding_id == binding_id)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return serialize_deployment_binding(dict(row)) if row else None

    def list_repository_deployment_bindings(
        self, workspace_id: str, repository_id: str
    ) -> list[JsonObject]:
        """같은 repo 를 바라보는 활성 바인딩 전부 — 글로벌 fan-out 대상 조회."""
        table = DeploymentBinding.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.repository_id == repository_id,
                table.c.status == DeploymentBindingStatus.ACTIVE.value,
            )
            .order_by(table.c.cluster_id, table.c.binding_id)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [serialize_deployment_binding(dict(row)) for row in rows]

    def list_workspace_deployment_bindings(self, workspace_id: str) -> list[JsonObject]:
        """워크스페이스의 활성 바인딩 전부 — 글로벌 그룹 탐색용(바인딩 수는 소규모)."""
        table = DeploymentBinding.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.status == DeploymentBindingStatus.ACTIVE.value,
            )
            .order_by(table.c.repository_id, table.c.app_name, table.c.cluster_id)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [serialize_deployment_binding(dict(row)) for row in rows]

    def list_active_github_poll_targets(
        self,
        workspace_id: str | None = None,
        *,
        limit: int = 500,
        binding_id: str | None = None,
    ) -> list[JsonObject]:
        """GitHub polling 대상 — DB에 등록된 앱/바인딩/watch target 기준.

        과거 바인딩에는 watch target row가 없을 수 있으므로 binding의 derived
        watch_target_id와 manifest_path를 fallback으로 사용한다.
        """
        repo_table = GitRepository.__table__
        watch_table = GitWatchTarget.__table__
        watch_by_id = watch_table.alias("watch_by_id")
        watch_by_source = watch_table.alias("watch_by_source")
        binding_table = DeploymentBinding.__table__
        app_table = Application.__table__
        binding_manifest_path = func.coalesce(
            binding_table.c.manifest_path,
            app_table.c.manifest_path,
        )
        branch = func.coalesce(
            watch_by_id.c.branch,
            watch_by_source.c.branch,
            repo_table.c.default_branch,
        ).label("branch")
        manifest_path = func.coalesce(
            watch_by_id.c.manifest_path,
            watch_by_source.c.manifest_path,
            binding_manifest_path,
        ).label("manifest_path")
        source_type = func.coalesce(
            watch_by_id.c.settings["source_type"].astext,
            watch_by_source.c.settings["source_type"].astext,
            binding_table.c.deploy_policy["manifest_source"].astext,
            binding_table.c.deploy_policy["source_type"].astext,
            app_table.c.metadata["source_type"].astext,
            "",
        ).label("source_type")
        watch_target_id = func.coalesce(
            watch_by_id.c.watch_target_id,
            watch_by_source.c.watch_target_id,
            binding_table.c.watch_target_id,
        ).label("watch_target_id")
        watch_status = func.coalesce(watch_by_id.c.status, watch_by_source.c.status)
        statement = (
            select(
                binding_table.c.workspace_id,
                app_table.c.application_id,
                repo_table.c.repository_id,
                repo_table.c.repo_ref,
                repo_table.c.credential_ref,
                branch,
                watch_target_id,
                binding_table.c.binding_id,
                binding_table.c.environment,
                binding_table.c.cluster_id,
                binding_table.c.namespace,
                binding_table.c.app_name,
                manifest_path,
                source_type,
                func.coalesce(
                    watch_by_id.c.last_seen_commit_sha,
                    watch_by_source.c.last_seen_commit_sha,
                ).label("last_seen_commit_sha"),
            )
            .select_from(binding_table)
            .join(
                repo_table,
                and_(
                    repo_table.c.workspace_id == binding_table.c.workspace_id,
                    repo_table.c.repository_id == binding_table.c.repository_id,
                ),
            )
            .join(
                app_table,
                and_(
                    app_table.c.workspace_id == binding_table.c.workspace_id,
                    app_table.c.repository_id == binding_table.c.repository_id,
                    app_table.c.name == binding_table.c.app_name,
                ),
            )
            .outerjoin(
                watch_by_id,
                and_(
                    watch_by_id.c.workspace_id == binding_table.c.workspace_id,
                    watch_by_id.c.repository_id == binding_table.c.repository_id,
                    watch_by_id.c.watch_target_id == binding_table.c.watch_target_id,
                ),
            )
            .outerjoin(
                watch_by_source,
                and_(
                    watch_by_id.c.watch_target_id.is_(None),
                    watch_by_source.c.workspace_id == binding_table.c.workspace_id,
                    watch_by_source.c.repository_id == binding_table.c.repository_id,
                    watch_by_source.c.branch == repo_table.c.default_branch,
                    watch_by_source.c.manifest_path == binding_manifest_path,
                ),
            )
            .where(
                repo_table.c.provider == GitProvider.GITHUB.value,
                repo_table.c.status == RepositoryStatus.ACTIVE.value,
                app_table.c.status == ApplicationStatus.ACTIVE.value,
                binding_table.c.status == DeploymentBindingStatus.ACTIVE.value,
                or_(
                    watch_status.is_(None),
                    watch_status == WatchTargetStatus.ACTIVE.value,
                ),
            )
            .order_by(repo_table.c.repo_ref, branch, binding_table.c.cluster_id)
            .limit(max(1, min(limit, 1000)))
        )
        if workspace_id is not None:
            statement = statement.where(binding_table.c.workspace_id == workspace_id)
        if binding_id is not None:
            statement = statement.where(binding_table.c.binding_id == binding_id)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [row_dict(row) for row in rows]

    def get_workflow_run(self, workflow_run_id: str) -> JsonObject | None:
        table = WorkflowRun.__table__
        statement = select(table).where(table.c.workflow_run_id == workflow_run_id).limit(1)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return serialize_workflow_run(dict(row)) if row else None

    def get_workflow_step_details(self, workflow_run_id: str, name: str) -> JsonObject | None:
        table = WorkflowRunStep.__table__
        statement = (
            select(table.c.details)
            .where(table.c.workflow_run_id == workflow_run_id, table.c.name == name)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return dict(row["details"] or {}) if row else None

    def latest_succeeded_run_for_binding(
        self, workspace_id: str, binding_id: str
    ) -> JsonObject | None:
        """바인딩의 최근 성공 run — 신규 클러스터 합류 시 초기 배포 기준."""
        table = WorkflowRun.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.binding_id == binding_id,
                table.c.status == WorkflowRunStatus.SUCCEEDED.value,
            )
            .order_by(table.c.updated_at.desc())
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return serialize_workflow_run(dict(row)) if row else None

    def start_workflow_run(self, payload: JsonObject) -> WorkflowMutation:
        workflow_run_id = derive_workflow_run_id(payload)
        workspace_id = str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID))
        application_id = derive_application_id(payload)
        binding_id = derive_deployment_binding_id(payload)
        table = WorkflowRun.__table__
        insert = pg_insert(table).values(
            workflow_run_id=workflow_run_id,
            workspace_id=workspace_id,
            application_id=application_id,
            binding_id=binding_id,
            environment=str(payload.get("environment", DEFAULT_ENVIRONMENT)),
            cluster_id=str(payload.get("cluster_id", "")),
            commit_sha=str(payload.get("commit_sha", "")),
            status=str(payload.get("status", WorkflowRunStatus.STARTED.value)),
            current_step=str(payload.get("current_step", WorkflowStepName.GIT.value)),
            summary=payload.get("summary"),
            command_id=payload.get("command_id"),
            metadata=dict(payload.get("metadata", {})),
            updated_at=func.now(),
        )
        # 생성은 무조건, 기존 행 갱신은 실제로 앞으로 가는 전이일 때만 허용한다.
        # 같은 상태의 재전달은 row를 반환하지 않아 후속 outbox/Timeline이 생기지 않는다.
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.workflow_run_id],
            set_={
                "status": insert.excluded.status,
                "current_step": insert.excluded.current_step,
                "summary": insert.excluded.summary,
                "command_id": insert.excluded.command_id,
                "metadata": insert.excluded.metadata,
                "updated_at": func.now(),
            },
            where=and_(
                workflow_transition_guard(table, insert.excluded.status),
                table.c.status != insert.excluded.status,
            ),
        ).returning(*(table.c[field] for field in WORKFLOW_MUTATION_FIELDS))
        with self.connection() as conn:
            result = conn.execute(statement)
        return workflow_mutation_from_result(result, fields=WORKFLOW_MUTATION_FIELDS)

    def update_workflow_run(self, payload: JsonObject) -> WorkflowMutation:
        workflow_run_id = derive_workflow_run_id(payload)
        values: JsonObject = {"updated_at": func.now()}
        for key in ("status", "current_step", "summary", "command_id"):
            if key in payload:
                values[key] = payload[key]
        if "metadata" in payload:
            values["metadata"] = dict(payload["metadata"])
        table = WorkflowRun.__table__
        statement = table.update().where(table.c.workflow_run_id == workflow_run_id)
        if "status" in values:
            # 상태 변경은 실제 앞으로의 전이일 때만 반영 — 재배달과 회귀를 모두 차단.
            statement = statement.where(
                workflow_transition_guard(table, str(values["status"])),
                table.c.status != str(values["status"]),
            )
        statement = statement.values(**values).returning(
            *(table.c[field] for field in WORKFLOW_MUTATION_FIELDS)
        )
        with self.connection() as conn:
            result = conn.execute(statement)
        return workflow_mutation_from_result(result, fields=WORKFLOW_MUTATION_FIELDS)

    def record_workflow_step(self, payload: JsonObject) -> WorkflowMutation:
        workflow_run_id = derive_workflow_run_id(payload)
        step_name = str(payload.get("name") or payload.get("step") or WorkflowStepName.GIT.value)
        step_id = str(payload.get("step_id") or derive_workflow_step_id(workflow_run_id, step_name))
        table = WorkflowRunStep.__table__
        insert = pg_insert(table).values(
            step_id=step_id,
            workflow_run_id=workflow_run_id,
            workspace_id=str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            application_id=derive_application_id(payload),
            binding_id=derive_deployment_binding_id(payload),
            environment=str(payload.get("environment", DEFAULT_ENVIRONMENT)),
            name=step_name,
            status=str(payload.get("status", WorkflowStepStatus.SUCCEEDED.value)),
            message=payload.get("message"),
            details=dict(payload.get("details", {})),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.workflow_run_id, table.c.name],
            set_={
                "status": insert.excluded.status,
                "message": insert.excluded.message,
                "details": insert.excluded.details,
                "updated_at": func.now(),
            },
            where=and_(
                workflow_step_transition_guard(table, insert.excluded.status),
                table.c.status != insert.excluded.status,
            ),
        ).returning(*(table.c[field] for field in WORKFLOW_STEP_MUTATION_FIELDS))
        with self.connection() as conn:
            result = conn.execute(statement)
        return workflow_mutation_from_result(result, fields=WORKFLOW_STEP_MUTATION_FIELDS)

    def request_workflow_approval(self, payload: JsonObject) -> JsonObject:
        workflow_run_id = derive_workflow_run_id(payload)
        approval_id = str(payload.get("approval_id") or derive_approval_id(workflow_run_id))
        table = Approval.__table__
        insert = pg_insert(table).values(
            approval_id=approval_id,
            workflow_run_id=workflow_run_id,
            workspace_id=str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            application_id=derive_application_id(payload),
            binding_id=derive_deployment_binding_id(payload),
            environment=str(payload.get("environment", DEFAULT_ENVIRONMENT)),
            status=str(payload.get("status", ApprovalStatus.REQUESTED.value)),
            reason=str(payload.get("reason", "")),
            requested_role=str(payload.get("requested_role", ResourceRole.RELEASE_OPERATOR.value)),
            requested_by=payload.get("requested_by"),
            decided_by=payload.get("decided_by"),
            decision=payload.get("decision"),
            details=dict(payload.get("details", {})),
            expires_at=payload.get("expires_at"),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[table.c.approval_id],
            set_={
                "status": insert.excluded.status,
                "reason": insert.excluded.reason,
                "requested_role": insert.excluded.requested_role,
                "requested_by": insert.excluded.requested_by,
                "decided_by": insert.excluded.decided_by,
                "decision": insert.excluded.decision,
                "details": insert.excluded.details,
                "expires_at": insert.excluded.expires_at,
                "updated_at": func.now(),
            },
            where=table.c.status.in_(OPEN_APPROVAL_STATUSES),
        )
        with self.connection() as conn:
            conn.execute(statement)
        return {**payload, "workflow_run_id": workflow_run_id, "approval_id": approval_id}

    def resolve_workflow_approval_if_open(
        self,
        approval_id: str,
        workspace_id: str,
        status: str,
        decided_by: str | None,
        decision: str | None,
        details: JsonObject,
    ) -> bool:
        """열린 승인만 원자적으로 해결함 — 동시 grant/reject 중 첫 요청만 성공.

        검사(open 여부)와 갱신이 한 UPDATE 라 read-then-write 경합이 없음.
        False 반환 = 이미 해결됨(호출자는 409 로 응답).

        만료 시각(expires_at)이 지난 승인은 열려 있어도 해결을 거부한다 —
        만료를 저장만 하고 검사하지 않으면 유효기간이 장식이 되기 때문.
        expires_at 이 없는 승인(기존 데이터 포함)은 현행대로 만료 없이 동작한다.
        """
        table = Approval.__table__
        statement = (
            table.update()
            .where(
                table.c.approval_id == approval_id,
                table.c.workspace_id == workspace_id,
                table.c.status.in_(OPEN_APPROVAL_STATUSES),
                or_(table.c.expires_at.is_(None), table.c.expires_at > func.now()),
            )
            .values(
                status=status,
                decided_by=decided_by,
                decision=decision,
                details=dict(details),
                updated_at=func.now(),
            )
            .returning(table.c.approval_id)
        )
        with self.connection() as conn:
            row = conn.execute(statement).first()
        return row is not None

    def resolve_workflow_approval(self, payload: JsonObject) -> JsonObject:
        workflow_run_id = derive_workflow_run_id(payload)
        approval_id = str(payload.get("approval_id") or derive_approval_id(workflow_run_id))
        table = Approval.__table__
        statement = (
            table.update()
            .where(table.c.approval_id == approval_id)
            .values(
                status=str(payload.get("status", ApprovalStatus.GRANTED.value)),
                decided_by=payload.get("decided_by"),
                decision=payload.get("decision"),
                details=dict(payload.get("details", {})),
                updated_at=func.now(),
            )
        )
        with self.connection() as conn:
            conn.execute(statement)
        return {**payload, "workflow_run_id": workflow_run_id, "approval_id": approval_id}

    def get_workflow_approval(
        self, approval_id: str, workspace_id: str = DEFAULT_WORKSPACE_ID
    ) -> JsonObject | None:
        table = Approval.__table__
        statement = (
            select(
                table.c.approval_id,
                table.c.workflow_run_id,
                table.c.workspace_id,
                table.c.application_id,
                table.c.binding_id,
                table.c.environment,
                table.c.status,
                table.c.reason,
                table.c.requested_role,
                table.c.requested_by,
                table.c.decided_by,
                table.c.decision,
                table.c.details,
                table.c.expires_at,
            )
            .where(table.c.approval_id == approval_id, table.c.workspace_id == workspace_id)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return dict(row) if row else None

    def count_open_workflow_approvals(
        self,
        workspace_id: str,
        application_ids: Collection[str] | None = None,
    ) -> int:
        """fleet 합계용 — 사람 결정 대기(requested) 승인 수.

        NOT_REQUIRED 는 자동 진행 표식이라 '대기'로 세지 않음(OPEN_APPROVAL_STATUSES 와 다른 기준).
        """
        if application_ids is not None and not application_ids:
            return 0
        table = Approval.__table__
        statement = select(func.count()).where(
            table.c.workspace_id == workspace_id,
            table.c.status == ApprovalStatus.REQUESTED.value,
        )
        if application_ids is not None:
            statement = statement.where(table.c.application_id.in_(tuple(sorted(application_ids))))
        with self.connection() as conn:
            return int(conn.execute(statement).scalar() or 0)

    def count_running_workflow_runs(
        self,
        workspace_id: str,
        application_ids: Collection[str] | None = None,
    ) -> int:
        """fleet 합계용 — 종결(SUCCEEDED/FAILED) 전 상태의 워크플로 run 수."""
        if application_ids is not None and not application_ids:
            return 0
        table = WorkflowRun.__table__
        statement = select(func.count()).where(
            table.c.workspace_id == workspace_id,
            table.c.status.not_in(TERMINAL_WORKFLOW_STATUSES),
        )
        if application_ids is not None:
            statement = statement.where(table.c.application_id.in_(tuple(sorted(application_ids))))
        with self.connection() as conn:
            return int(conn.execute(statement).scalar() or 0)

    def workflow_run_status_counts(
        self, workspace_id: str = DEFAULT_WORKSPACE_ID
    ) -> dict[str, int]:
        table = WorkflowRun.__table__
        statement = (
            select(table.c.status, func.count().label("count"))
            .where(table.c.workspace_id == workspace_id)
            .group_by(table.c.status)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).all()
        return {str(row[0]): int(row[1] or 0) for row in rows}

    def workflow_run_current_step_counts(
        self, workspace_id: str = DEFAULT_WORKSPACE_ID
    ) -> dict[str, int]:
        table = WorkflowRun.__table__
        statement = (
            select(table.c.current_step, func.count().label("count"))
            .where(
                table.c.workspace_id == workspace_id,
                table.c.status.not_in(TERMINAL_WORKFLOW_STATUSES),
            )
            .group_by(table.c.current_step)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).all()
        return {str(row[0]): int(row[1] or 0) for row in rows}

    def attach_workflow_command(self, workflow_run_id: str, command_id: str) -> None:
        table = WorkflowRun.__table__
        statement = (
            table.update()
            .where(table.c.workflow_run_id == workflow_run_id)
            .values(command_id=command_id, updated_at=func.now())
        )
        with self.connection() as conn:
            conn.execute(statement)

    def get_workflow_command_progress(self, workflow_run_id: str) -> JsonObject:
        """Return every approval and durable agent command owned by one workflow.

        A WorkflowRun is commit-scoped while approvals and commands are
        resource-scoped.  Completion must therefore be derived from this full
        set rather than from ``workflow_runs.command_id`` (a legacy display
        pointer that can name only one command).
        """

        approval = Approval.__table__
        command = AgentCommand.__table__
        with self.connection() as conn:
            approval_rows = (
                conn.execute(
                    select(
                        approval.c.approval_id,
                        approval.c.status,
                        approval.c.details,
                    )
                    .where(approval.c.workflow_run_id == workflow_run_id)
                    .order_by(approval.c.approval_id)
                )
                .mappings()
                .all()
            )
            command_rows = (
                conn.execute(
                    select(
                        command.c.command_id,
                        command.c.status,
                        command.c.payload,
                        command.c.result,
                        command.c.completed_at,
                    )
                    .where(command.c.payload["workflow_run_id"].astext == workflow_run_id)
                    .order_by(command.c.command_id)
                )
                .mappings()
                .all()
            )
        return {
            "approvals": [
                {
                    "approval_id": str(row["approval_id"]),
                    "status": str(row["status"]),
                    "details": dict(row["details"] or {}),
                }
                for row in approval_rows
            ],
            "commands": [
                {
                    "command_id": str(row["command_id"]),
                    "approval_ref": str((row["payload"] or {}).get("approval_ref") or ""),
                    "status": str(row["status"]),
                    "payload": dict(row["payload"] or {}),
                    "result": dict(row["result"] or {}),
                    "completed_at": iso_or_none(row["completed_at"]),
                }
                for row in command_rows
            ],
        }

    def record_approved_workflow_snapshots(self, workflow_run_id: str) -> int:
        """CAS-upsert supported resource snapshots after the workflow succeeded.

        The binding policy is user configuration and may be replaced by a later
        registration update, so runtime Git authority lives in its own table.
        The completion timestamp guard prevents an older delayed workflow from
        overwriting a newer approved resource state.  The return value counts
        every validated command, including kinds for which managed-field
        snapshots are not supported.  A successfully applied mixed manifest
        must not fail merely because (for example) a PodDisruptionBudget has no
        fields in the current GitOps diff model.
        """

        run = WorkflowRun.__table__
        approval = Approval.__table__
        command = AgentCommand.__table__
        snapshot_table = ApprovedResourceSnapshot.__table__
        with self.connection() as conn:
            run_row = (
                conn.execute(
                    select(
                        run.c.workflow_run_id,
                        run.c.workspace_id,
                        run.c.binding_id,
                        run.c.cluster_id,
                        run.c.commit_sha,
                    )
                    .where(run.c.workflow_run_id == workflow_run_id)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if run_row is None:
                return 0
            approval_rows = (
                conn.execute(
                    select(approval.c.approval_id, approval.c.status).where(
                        approval.c.workflow_run_id == workflow_run_id
                    )
                )
                .mappings()
                .all()
            )
            command_rows = (
                conn.execute(
                    select(
                        command.c.command_id,
                        command.c.status,
                        command.c.payload,
                        command.c.result,
                        command.c.completed_at,
                    ).where(command.c.payload["workflow_run_id"].astext == workflow_run_id)
                )
                .mappings()
                .all()
            )
            granted = {
                str(row["approval_id"])
                for row in approval_rows
                if str(row["status"]) == ApprovalStatus.GRANTED.value
            }
            if approval_rows and len(granted) != len(approval_rows):
                return 0
            command_approvals = {
                str((row["payload"] or {}).get("approval_ref") or "") for row in command_rows
            }
            if granted and not granted.issubset(command_approvals):
                return 0
            if not command_rows or any(
                str(row["status"]) != CommandStatus.COMPLETED
                or not promotion_gate_from_command_result(dict(row["result"] or {}))["eligible"]
                for row in command_rows
            ):
                return 0

            prepared_result = prepare_approved_resource_snapshots(
                workflow_run_id,
                run_row,
                command_rows,
            )
            if prepared_result is None:
                return 0
            handled, prepared = prepared_result
            for values in prepared:
                insert = pg_insert(snapshot_table).values(**values)
                statement = insert.on_conflict_do_update(
                    index_elements=[
                        snapshot_table.c.workspace_id,
                        snapshot_table.c.binding_id,
                        snapshot_table.c.cluster_id,
                        snapshot_table.c.namespace,
                        snapshot_table.c.resource_kind,
                        snapshot_table.c.resource_name,
                    ],
                    set_={
                        "workflow_run_id": insert.excluded.workflow_run_id,
                        "command_id": insert.excluded.command_id,
                        "commit_sha": insert.excluded.commit_sha,
                        "artifact_digest": insert.excluded.artifact_digest,
                        "managed_fields": insert.excluded.managed_fields,
                        "snapshot": insert.excluded.snapshot,
                        "completed_at": insert.excluded.completed_at,
                        "updated_at": func.now(),
                    },
                    where=snapshot_table.c.completed_at <= insert.excluded.completed_at,
                )
                conn.execute(statement)
        return handled

    def get_last_approved_resource_snapshot(
        self,
        workspace_id: str,
        binding_id: str,
        cluster_id: str,
        namespace: str,
        resource: str,
    ) -> JsonObject | None:
        normalized = resource.strip().casefold()
        if "/" not in normalized:
            return None
        kind, name = normalized.split("/", 1)
        table = ApprovedResourceSnapshot.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.binding_id == binding_id,
                table.c.cluster_id == cluster_id,
                table.c.namespace == namespace,
                table.c.resource_kind == kind,
                table.c.resource_name == name,
            )
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        if row is None:
            return None
        result = row_dict(row)
        result["managed_fields"] = dict(result.get("managed_fields") or {})
        result["snapshot"] = dict(result.get("snapshot") or {})
        result["completed_at"] = iso_or_none(result.get("completed_at"))
        return result

    def update_workflow_run_for_command(self, payload: JsonObject) -> WorkflowMutation:
        command_id = str(payload["command_id"])
        values: JsonObject = {"updated_at": func.now()}
        for key in ("status", "current_step", "summary"):
            if key in payload:
                values[key] = payload[key]
        if "metadata" in payload:
            values["metadata"] = dict(payload["metadata"])
        table = WorkflowRun.__table__
        statement = table.update().where(table.c.command_id == command_id)
        if "status" in values:
            # 상태 변경은 실제 앞으로의 전이일 때만 반영 — 재배달과 회귀를 모두 차단.
            statement = statement.where(
                workflow_transition_guard(table, str(values["status"])),
                table.c.status != str(values["status"]),
            )
        statement = statement.values(**values).returning(
            *(table.c[field] for field in WORKFLOW_MUTATION_FIELDS)
        )
        with self.connection() as conn:
            result = conn.execute(statement)
        return workflow_mutation_from_result(result, fields=WORKFLOW_MUTATION_FIELDS)

    def get_workflow_identity_for_command(self, command_id: str) -> JsonObject | None:
        table = WorkflowRun.__table__
        statement = (
            select(
                table.c.workflow_run_id,
                table.c.workspace_id,
                table.c.application_id,
                table.c.binding_id,
                table.c.environment,
                table.c.cluster_id,
                table.c.commit_sha,
            )
            .where(table.c.command_id == command_id)
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        if row:
            return dict(row)
        return self._workflow_identity_from_queued_command(command_id)

    def _workflow_identity_from_queued_command(self, command_id: str) -> JsonObject | None:
        table = AgentCommand.__table__
        statement = select(table.c.payload).where(table.c.command_id == command_id).limit(1)
        with self.connection() as conn:
            payload = conn.execute(statement).scalar_one_or_none()
        if not isinstance(payload, dict):
            return None
        workflow_run_id = payload.get("workflow_run_id")
        application_id = payload.get("application_id")
        if not workflow_run_id or not application_id:
            return None
        return {
            "workflow_run_id": str(workflow_run_id),
            "workspace_id": str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            "application_id": str(application_id),
            "binding_id": str(payload.get("binding_id", DEFAULT_DEPLOYMENT_BINDING_ID)),
            "environment": str(payload.get("environment", DEFAULT_ENVIRONMENT)),
            "cluster_id": str(payload.get("cluster_id", Target.DEFAULT_CLUSTER_ID)),
            "commit_sha": str(payload.get("commit_sha", "")),
        }

    def save_repo_change(
        self,
        correlation_id: str,
        commit_sha: str,
        manifest: JsonObject,
        workspace_id: str = DEFAULT_WORKSPACE_ID,
        repository_id: str | None = None,
        watch_target_id: str | None = None,
        binding_id: str | None = None,
        manifest_path: str | None = None,
    ) -> None:
        table = RepoChange.__table__
        statement = pg_insert(table).values(
            workspace_id=workspace_id,
            correlation_id=correlation_id,
            commit_sha=commit_sha,
            repository_id=repository_id,
            watch_target_id=watch_target_id,
            binding_id=binding_id,
            manifest_path=manifest_path,
            manifest=manifest,
        )
        with self.connection() as conn:
            conn.execute(statement)

    # cost: time O(log n), heap O(1), stack O(1), io 1
    # vars: n = Git source 관측 행 수
    # basis: estimate
    def get_git_source_observation(
        self,
        workspace_id: str,
        binding_id: str,
        commit_sha: str,
        manifest_path: str,
    ) -> JsonObject | None:
        table = GitSourceObservation.__table__
        statement = select(table).where(
            table.c.workspace_id == workspace_id,
            table.c.binding_id == binding_id,
            table.c.commit_sha == commit_sha,
            table.c.manifest_path == manifest_path,
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return row_dict(row) if row is not None else None

    # cost: time O(log n), heap O(1), stack O(1), io 1-2
    # vars: n = Git source 관측 행 수
    # basis: estimate
    def record_git_source_observation(self, payload: JsonObject) -> JsonObject:
        table = GitSourceObservation.__table__
        insert = pg_insert(table).values(
            workspace_id=str(payload["workspace_id"]),
            binding_id=str(payload["binding_id"]),
            repository_id=str(payload["repository_id"]),
            application_id=str(payload["application_id"]),
            cluster_id=str(payload["cluster_id"]),
            namespace=str(payload["namespace"]),
            deployment_name=str(payload["deployment_name"]),
            repo_ref=str(payload["repo_ref"]),
            commit_sha=str(payload["commit_sha"]),
            manifest_path=str(payload["manifest_path"]),
            source_type=str(payload["source_type"]),
            status=str(payload["status"]),
            image=payload.get("image"),
            replicas=payload.get("replicas"),
            source_sha256=payload.get("source_sha256"),
            reason=payload.get("reason"),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[
                table.c.workspace_id,
                table.c.binding_id,
                table.c.commit_sha,
                table.c.manifest_path,
            ],
            set_={
                "repository_id": insert.excluded.repository_id,
                "application_id": insert.excluded.application_id,
                "cluster_id": insert.excluded.cluster_id,
                "namespace": insert.excluded.namespace,
                "deployment_name": insert.excluded.deployment_name,
                "repo_ref": insert.excluded.repo_ref,
                "source_type": insert.excluded.source_type,
                "status": insert.excluded.status,
                "image": insert.excluded.image,
                "replicas": insert.excluded.replicas,
                "source_sha256": insert.excluded.source_sha256,
                "reason": insert.excluded.reason,
                "observed_at": case(
                    (insert.excluded.status == "observed", func.now()),
                    else_=table.c.observed_at,
                ),
                "updated_at": func.now(),
            },
            where=table.c.status != "observed",
        ).returning(table)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
            if row is None:
                row = (
                    conn.execute(
                        select(table).where(
                            table.c.workspace_id == payload["workspace_id"],
                            table.c.binding_id == payload["binding_id"],
                            table.c.commit_sha == payload["commit_sha"],
                            table.c.manifest_path == payload["manifest_path"],
                        )
                    )
                    .mappings()
                    .one()
                )
        saved = row_dict(row)
        if (
            payload["status"] == "observed"
            and saved["status"] == "observed"
            and saved["source_sha256"] != payload.get("source_sha256")
        ):
            raise ValueError("git source changed at the same commit")
        return saved

    def record_manifest_artifact(self, payload: JsonObject) -> JsonObject:
        repository_id = derive_repository_id(payload)
        watch_target_id = derive_watch_target_id({**payload, "repository_id": repository_id})
        binding_id = derive_deployment_binding_id(payload)
        artifact_id = str(
            payload.get("artifact_id")
            or manifest_artifact_id({**payload, "binding_id": binding_id})
        )
        table = ManifestArtifact.__table__
        insert = pg_insert(table).values(
            artifact_id=artifact_id,
            workspace_id=str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            repository_id=repository_id,
            watch_target_id=watch_target_id,
            binding_id=binding_id,
            commit_sha=str(payload["commit_sha"]),
            manifest_path=str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
            status=str(payload.get("status", ManifestArtifactStatus.RENDERED.value)),
            status_reason=payload.get("status_reason"),
            rendered_manifest=payload.get("rendered_manifest"),
            source_summary=dict(payload.get("source_summary", {})),
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[
                table.c.workspace_id,
                table.c.binding_id,
                table.c.commit_sha,
                table.c.manifest_path,
            ],
            set_={
                "status": insert.excluded.status,
                "status_reason": insert.excluded.status_reason,
                "rendered_manifest": insert.excluded.rendered_manifest,
                "source_summary": insert.excluded.source_summary,
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)
        return {
            **payload,
            "artifact_id": artifact_id,
            "repository_id": repository_id,
            "watch_target_id": watch_target_id,
            "binding_id": binding_id,
        }

    def find_rendered_manifest_artifacts(
        self,
        workspace_id: str,
        binding_id: str,
        commit_sha: str,
        manifest_path: str,
        renderer_version: str,
    ) -> list[JsonObject]:
        table = ManifestArtifact.__table__
        prefix = f"{manifest_path}#"
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.binding_id == binding_id,
                table.c.commit_sha == commit_sha,
                table.c.status == ManifestArtifactStatus.RENDERED.value,
                table.c.rendered_manifest.is_not(None),
                or_(
                    table.c.manifest_path == manifest_path,
                    table.c.manifest_path.like(f"{prefix}%"),
                ),
            )
            .order_by(table.c.manifest_path.asc())
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()

        artifacts: list[JsonObject] = []
        for row in rows:
            artifact = row_dict(row)
            source_summary = artifact.get("source_summary", {})
            if not isinstance(source_summary, dict):
                continue
            if source_summary.get("renderer_version") != renderer_version:
                continue
            artifacts.append(artifact)
        return artifacts

    def list_owned_resource_identities(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        exclude_application_id: str | None = None,
    ) -> dict[str, dict[str, str]]:
        """클러스터에서 활성 추적 대상이 이미 소유한 리소스 식별자 → 소유 앱 매핑.

        연결 시점의 '소유권 겹침' 감지에 쓴다. 활성 바인딩의 렌더된 매니페스트
        아티팩트에서 resource_identity 를 모아 인덱스로 만든다. 재연결 중인 앱은
        exclude_application_id 로 제외해 자기 자신과의 충돌을 피한다.

        주의: 오래된 커밋의 아티팩트가 남아 있으면 과다 보고될 수 있다(경고 성격).
        하드 차단이 아니라 사용자 확인(override)로 진행 가능하게 설계한다.
        """
        art = ManifestArtifact.__table__
        binding = DeploymentBinding.__table__
        rid = art.c.source_summary["resource_identity"].astext
        app_id_col = art.c.source_summary["application_id"].astext
        statement = (
            select(
                rid.label("rid"),
                app_id_col.label("app_id"),
                binding.c.app_name.label("app_name"),
            )
            .select_from(art.join(binding, art.c.binding_id == binding.c.binding_id))
            .where(
                art.c.workspace_id == workspace_id,
                binding.c.cluster_id == cluster_id,
                binding.c.status == DeploymentBindingStatus.ACTIVE.value,
                art.c.status == ManifestArtifactStatus.RENDERED.value,
                rid.is_not(None),
            )
        )
        index: dict[str, dict[str, str]] = {}
        with self.connection() as conn:
            for row in conn.execute(statement).mappings():
                key = str(row["rid"] or "")
                owner_app_id = str(row["app_id"] or "")
                if not key:
                    continue
                if exclude_application_id and owner_app_id == exclude_application_id:
                    continue
                index[key] = {
                    "application_id": owner_app_id,
                    "app_name": str(row["app_name"] or ""),
                }
        return index

    def get_manifest_artifact_provenance(
        self,
        workspace_id: str,
        binding_id: str,
        commit_sha: str,
        manifest_path: str,
        resource: str,
        artifact_digest: str,
    ) -> JsonObject | None:
        """워크플로 diff와 정확히 결합된 manifest source provenance만 반환한다."""

        table = ManifestArtifact.__table__
        prefix = f"{manifest_path}#"
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.binding_id == binding_id,
                table.c.commit_sha == commit_sha,
                table.c.status == ManifestArtifactStatus.RENDERED.value,
                table.c.rendered_manifest.is_not(None),
                table.c.manifest_path.like(f"{prefix}%"),
            )
            .order_by(table.c.manifest_path.asc())
        )
        with self.connection() as conn:
            rows = [row_dict(row) for row in conn.execute(statement).mappings().all()]
        if not rows:
            return None

        expected_path = f"{manifest_path}#{resource}"
        target = next((row for row in rows if row.get("manifest_path") == expected_path), None)
        if target is None:
            return None
        rendered = target.get("rendered_manifest")
        if not isinstance(rendered, dict) or rendered.get("artifact_digest") != artifact_digest:
            return None
        desired_manifest = rendered.get("manifest")
        if not isinstance(desired_manifest, dict) or not desired_manifest:
            return None

        summaries = [row.get("source_summary") for row in rows]
        if any(not isinstance(summary, dict) for summary in summaries):
            return None
        target_summary = target.get("source_summary")
        if not isinstance(target_summary, dict):
            return None
        source_summary = dict(target_summary)
        source_document_count = source_summary.get("source_document_count")
        common_summary = {
            key: value
            for key, value in source_summary.items()
            if key not in {"resource", "source_manifest_sha256"}
        }
        if (
            isinstance(source_document_count, bool)
            or not isinstance(source_document_count, int)
            or source_document_count != len(rows)
            or any(
                {
                    key: value
                    for key, value in dict(summary).items()
                    if key not in {"resource", "source_manifest_sha256"}
                }
                != common_summary
                for summary in summaries
            )
        ):
            return None

        return {
            **source_summary,
            "workspace_id": workspace_id,
            "repository_id": str(target.get("repository_id") or ""),
            "binding_id": binding_id,
            "commit_sha": commit_sha,
            "manifest_path": manifest_path,
            "artifact_manifest_path": expected_path,
            "artifact_digest": artifact_digest,
            "artifact_count": len(rows),
            "desired_manifest": dict(desired_manifest),
        }

    def mark_watch_observed(
        self,
        watch_target_id: str,
        commit_sha: str,
        workspace_id: str = DEFAULT_WORKSPACE_ID,
        repository_id: str = DEFAULT_REPOSITORY_ID,
        branch: str = DEFAULT_REPO_BRANCH,
        manifest_path: str = DEFAULT_MANIFEST_PATH,
    ) -> None:
        table = GitWatchTarget.__table__
        insert = pg_insert(table).values(
            watch_target_id=watch_target_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            branch=branch,
            manifest_path=manifest_path,
            interval_seconds=30,
            last_seen_commit_sha=commit_sha,
            last_polled_at=func.now(),
            status=WatchTargetStatus.ACTIVE.value,
            settings={},
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[
                table.c.workspace_id,
                table.c.repository_id,
                table.c.branch,
                table.c.manifest_path,
            ],
            set_={
                "last_seen_commit_sha": insert.excluded.last_seen_commit_sha,
                "last_polled_at": func.now(),
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)

    def record_watch_poll_result(
        self,
        watch_target_id: str,
        *,
        workspace_id: str = DEFAULT_WORKSPACE_ID,
        repository_id: str = DEFAULT_REPOSITORY_ID,
        branch: str = DEFAULT_REPO_BRANCH,
        manifest_path: str = DEFAULT_MANIFEST_PATH,
        ok: bool = True,
        status_code: int | None = None,
        error_kind: str = "",
        error: str = "",
    ) -> None:
        table = GitWatchTarget.__table__
        settings = {
            "poll_status": "ok" if ok else "failed",
            "poll_status_code": status_code,
            "poll_error_kind": "" if ok else error_kind,
            "poll_error": "" if ok else error[:500],
        }
        insert = pg_insert(table).values(
            watch_target_id=watch_target_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            branch=branch,
            manifest_path=manifest_path,
            interval_seconds=30,
            last_polled_at=func.now(),
            status=WatchTargetStatus.ACTIVE.value,
            settings=settings,
            updated_at=func.now(),
        )
        statement = insert.on_conflict_do_update(
            index_elements=[
                table.c.workspace_id,
                table.c.repository_id,
                table.c.branch,
                table.c.manifest_path,
            ],
            set_={
                "last_polled_at": func.now(),
                "settings": table.c.settings.op("||")(insert.excluded.settings),
                "updated_at": func.now(),
            },
        )
        with self.connection() as conn:
            conn.execute(statement)

    def get_watch_last_seen_commit_sha(
        self, watch_target_id: str, workspace_id: str = DEFAULT_WORKSPACE_ID
    ) -> str | None:
        table = GitWatchTarget.__table__
        statement = (
            select(table.c.last_seen_commit_sha)
            .where(table.c.watch_target_id == watch_target_id, table.c.workspace_id == workspace_id)
            .limit(1)
        )
        with self.connection() as conn:
            value = conn.execute(statement).scalar_one_or_none()
        return str(value) if value else None

    def _grant_owner_if_present(
        self,
        workspace_id: str,
        user_id: object,
        resource_type: str,
        resource_id: str,
    ) -> None:
        if not user_id:
            return
        grant = getattr(self, "grant_resource_access", None)
        if callable(grant):
            grant(
                {
                    "workspace_id": workspace_id,
                    "subject_id": str(user_id),
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "role": ResourceRole.CLUSTER_STEWARD.value,
                }
            )


def manifest_artifact_id(payload: JsonObject) -> str:
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            str(payload.get("binding_id", DEFAULT_DEPLOYMENT_BINDING_ID)),
            str(payload["commit_sha"]),
            str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
        ]
    )
    return f"manifest-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def derive_repository_id(payload: JsonObject) -> str:
    explicit = payload.get("repository_id")
    if explicit and explicit != DEFAULT_REPOSITORY_ID:
        return str(explicit)
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            str(payload.get("repo_ref", DEFAULT_REPO_REF)),
        ]
    )
    return f"repo-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def stable_credential_id(workspace_id: str, provider: str, scope: str) -> str:
    raw = "|".join([workspace_id, provider, scope])
    return f"cred-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def repository_credential_scope(repository_id: str) -> str:
    """Return the canonical vault scope for one GitHub repository."""
    return f"{GITHUB_REPOSITORY_CREDENTIAL_SCOPE_PREFIX}:{repository_id}"


def derive_watch_target_id(payload: JsonObject) -> str:
    explicit = payload.get("watch_target_id")
    if explicit and explicit != DEFAULT_WATCH_TARGET_ID:
        return str(explicit)
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            derive_repository_id(payload),
            str(payload.get("branch", DEFAULT_REPO_BRANCH)),
            str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
        ]
    )
    return f"watch-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def derive_deployment_binding_id(payload: JsonObject) -> str:
    explicit = payload.get("binding_id")
    if explicit and explicit != DEFAULT_DEPLOYMENT_BINDING_ID:
        return str(explicit)
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            derive_repository_id(payload),
            str(payload.get("cluster_id", Target.DEFAULT_CLUSTER_ID)),
            str(payload.get("namespace", "sandbox")),
            str(payload.get("app_name", DEFAULT_APPLICATION_ID)),
        ]
    )
    return f"binding-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def derive_application_id(payload: JsonObject) -> str:
    explicit = payload.get("application_id")
    if explicit and explicit != DEFAULT_APPLICATION_ID:
        return str(explicit)
    application_name = derive_application_name(payload) or DEFAULT_APPLICATION_ID
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            derive_repository_id(payload),
            str(payload.get("manifest_path", DEFAULT_MANIFEST_PATH)),
            application_name,
        ]
    )
    return f"app-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def application_identity_lock_key(workspace_id: str, repository_id: str, name: str) -> int:
    raw = f"application\0{workspace_id}\0{repository_id}\0{name}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], byteorder="big", signed=True)


def repository_identity_lock_key(workspace_id: str, repo_ref: str) -> int:
    canonical_repo_ref = normalize_github_repo_ref(repo_ref)
    raw = f"repository\0{workspace_id}\0{canonical_repo_ref}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], byteorder="big", signed=True)


def workspace_credential_lock_key(workspace_id: str, provider: str, scope: str) -> int:
    raw = f"workspace-credential\0{workspace_id}\0{provider}\0{scope}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], byteorder="big", signed=True)


def derive_application_name(payload: JsonObject) -> str:
    explicit = payload.get("name") or payload.get("app_name")
    if explicit:
        return str(explicit)
    resource = str(payload.get("resource", ""))
    if "/" in resource:
        return resource.split("/", 1)[1]
    repo_ref = str(payload.get("repo_ref", ""))
    if "/" in repo_ref:
        return repo_ref.rsplit("/", 1)[1]
    return ""


def derive_workflow_run_id(payload: JsonObject) -> str:
    explicit = payload.get("workflow_run_id")
    if explicit and explicit != DEFAULT_WORKFLOW_RUN_ID:
        return str(explicit)
    raw = "|".join(
        [
            str(payload.get("workspace_id", DEFAULT_WORKSPACE_ID)),
            derive_application_id(payload),
            derive_deployment_binding_id(payload),
            str(payload.get("environment", DEFAULT_ENVIRONMENT)),
            str(payload.get("commit_sha", "")),
        ]
    )
    return f"workflow-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def derive_workflow_step_id(workflow_run_id: str, step_name: str) -> str:
    raw = f"{workflow_run_id}|{step_name}"
    return f"step-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def derive_approval_id(workflow_run_id: str, qualifier: str | None = None) -> str:
    suffix = qualifier or "deploy-approval"
    raw = f"{workflow_run_id}|{suffix}"
    return f"approval-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def serialize_application(row: Any) -> JsonObject:
    item = dict(row)
    item["metadata"] = dict(item.get("metadata") or {})
    item["created_at"] = iso_or_none(item.get("created_at"))
    item["updated_at"] = iso_or_none(item.get("updated_at"))
    return item


def serialize_deployment_binding(row: Any) -> JsonObject:
    item = dict(row)
    item["deploy_policy"] = dict(item.get("deploy_policy") or {})
    item["access_policy"] = dict(item.get("access_policy") or {})
    watch_settings = item.pop("watch_settings", None)
    watch_last_seen_commit_sha = item.pop("watch_last_seen_commit_sha", None)
    watch_last_polled_at = item.pop("watch_last_polled_at", None)
    if (
        watch_settings is not None
        or watch_last_seen_commit_sha is not None
        or watch_last_polled_at is not None
    ):
        settings = dict(watch_settings or {})
        item["gitops_poll"] = {
            "status": str(settings.get("poll_status") or "unknown"),
            "status_code": settings.get("poll_status_code"),
            "error_kind": str(settings.get("poll_error_kind") or ""),
            "error": str(settings.get("poll_error") or ""),
            "last_seen_commit_sha": str(watch_last_seen_commit_sha or ""),
            "last_polled_at": iso_or_none(watch_last_polled_at),
        }
    item["created_at"] = iso_or_none(item.get("created_at"))
    item["updated_at"] = iso_or_none(item.get("updated_at"))
    return item


def serialize_workflow_run(row: Any) -> JsonObject:
    item = dict(row)
    item["metadata"] = dict(item.get("metadata") or {})
    result = item["metadata"].get("result")
    if isinstance(result, dict):
        item["promotion_gate"] = promotion_gate_from_command_result(result)
    item["created_at"] = iso_or_none(item.get("created_at"))
    item["updated_at"] = iso_or_none(item.get("updated_at"))
    return item


def current_workflow_approval(approvals: list[JsonObject]) -> JsonObject | None:
    """Run list 대표 approval — 열린 approval 우선, 없으면 최신 approval.

    Approval id는 resource qualifier를 포함할 수 있어 클라이언트가 추정하면 안 된다.
    """
    if not approvals:
        return None
    for status in OPEN_APPROVAL_STATUSES:
        for approval in approvals:
            if str(approval.get("status")) == status:
                return approval
    return approvals[0]
