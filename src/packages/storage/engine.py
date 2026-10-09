from __future__ import annotations

from contextlib import AbstractContextManager, asynccontextmanager, contextmanager, nullcontext
from contextvars import ContextVar
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from packages.config.constants import CommandStatus
from packages.config.settings import env, required_env
from packages.contracts.event_bus.interfaces import JsonObject
from packages.contracts.identity import (
    DEFAULT_WORKSPACE_ID,
    GLOBAL_ROLE_POLICY_ORGANIZATION_ID,
    Permission,
    ResourceRole,
    ServiceRole,
)
from packages.storage.schema import (
    metadata,
)

DATABASE_URL_ENV = "DATABASE_URL"
ERROR_MESSAGE_LIMIT = 2000
# 트랜잭션 안전 타임아웃 — 워크로드 특성이 다른 배포는 env(ms 단위)로 오버라이드 가능함.
# env 미설정 시 기존 기본값(5s/30s/30s)과 같은 의미의 ms 값이 적용됨(배포 호환).
DB_LOCK_TIMEOUT_MS_ENV = "DB_LOCK_TIMEOUT_MS"  # 잠금 대기 한도 ms(기본 5000)
DB_STATEMENT_TIMEOUT_MS_ENV = "DB_STATEMENT_TIMEOUT_MS"  # 쿼리 실행 한도 ms(기본 30000)
DB_IDLE_IN_TRANSACTION_TIMEOUT_MS_ENV = (
    "DB_IDLE_IN_TRANSACTION_TIMEOUT_MS"  # 유휴 트랜잭션 한도 ms(기본 30000)
)
DB_SCHEMA_INIT_LOCK_TIMEOUT_MS_ENV = (
    "DB_SCHEMA_INIT_LOCK_TIMEOUT_MS"  # 스키마 초기화 잠금 대기 한도 ms(기본 60000)
)
DB_SCHEMA_INIT_STATEMENT_TIMEOUT_MS_ENV = (
    "DB_SCHEMA_INIT_STATEMENT_TIMEOUT_MS"  # 스키마 초기화 DDL 실행 한도 ms(기본 120000)
)
DB_LOCK_TIMEOUT = f"{int(env(DB_LOCK_TIMEOUT_MS_ENV, '5000'))}ms"
DB_STATEMENT_TIMEOUT = f"{int(env(DB_STATEMENT_TIMEOUT_MS_ENV, '30000'))}ms"
DB_IDLE_IN_TRANSACTION_TIMEOUT = f"{int(env(DB_IDLE_IN_TRANSACTION_TIMEOUT_MS_ENV, '30000'))}ms"
DB_SCHEMA_INIT_LOCK_TIMEOUT = f"{int(env(DB_SCHEMA_INIT_LOCK_TIMEOUT_MS_ENV, '60000'))}ms"
DB_SCHEMA_INIT_STATEMENT_TIMEOUT = (
    f"{int(env(DB_SCHEMA_INIT_STATEMENT_TIMEOUT_MS_ENV, '120000'))}ms"
)
# 스키마 초기화 advisory lock 식별자 — 여러 워커가 동시에 create_all/호환 마이그레이션을
# 실행하지 못하게 전 배포가 같은 (namespace, key)로 pg_advisory_xact_lock 을 잡음.
# 모든 프로세스가 동일 잠금을 공유해야 상호 배제가 성립하므로 env 오버라이드 없는 고정값임.
SCHEMA_INIT_LOCK_NAMESPACE = 774897281
SCHEMA_INIT_LOCK_KEY = 20260703
POSTGRES_REQUIRED_EXTENSIONS = ("pg_trgm",)

# 상태 어휘(흩어진 리터럴 단일화)
DEAD_LETTER_STATUS_OPEN = "open"
DEAD_LETTER_STATUS_REPLAYED = "replayed"
DEAD_LETTER_STATUS_ARCHIVED = "archived"
RAW_DEAD_LETTER_SUBJECT = "__decode_failed__"
UNKNOWN_AGENT_ID = "unknown-agent"
AGENT_COMMAND_COMPAT_COLUMNS = {
    "lease_id": "alter table agent_commands add column if not exists lease_id text",
    "agent_id": "alter table agent_commands add column if not exists agent_id text",
    "priority": (
        "alter table agent_commands add column if not exists priority integer not null default 100"
    ),
    "leased_until": "alter table agent_commands add column if not exists leased_until timestamptz",
    "started_at": "alter table agent_commands add column if not exists started_at timestamptz",
    "completed_at": "alter table agent_commands add column if not exists completed_at timestamptz",
    "confirmation_event_id": (
        "alter table agent_commands add column if not exists confirmation_event_id text"
    ),
    "impact_identity": "alter table agent_commands add column if not exists impact_identity text",
    "direct_execution": (
        "alter table agent_commands add column if not exists direct_execution boolean not null default false"
    ),
    "attempt_count": (
        "alter table agent_commands add column if not exists attempt_count integer not null default 0"
    ),
    "active_attempt_id": "alter table agent_commands add column if not exists active_attempt_id text",
    "cancel_requested_at": (
        "alter table agent_commands add column if not exists cancel_requested_at timestamptz"
    ),
    "cancel_requested_by": (
        "alter table agent_commands add column if not exists cancel_requested_by text"
    ),
    "cancel_reason": "alter table agent_commands add column if not exists cancel_reason text",
    "cancel_accepted_at": (
        "alter table agent_commands add column if not exists cancel_accepted_at timestamptz"
    ),
    "cancel_generation": (
        "alter table agent_commands add column if not exists cancel_generation integer not null default 0"
    ),
    "terminal_event_id": "alter table agent_commands add column if not exists terminal_event_id text",
}
EVENT_COMPAT_COLUMNS = {
    "causation_id": "alter table events add column if not exists causation_id text",
    "schema_version": (
        "alter table events add column if not exists schema_version integer not null default 1"
    ),
}
EVENT_PROCESSING_COMPAT_COLUMNS = {
    "processing_duration_ms": (
        "alter table event_processing add column if not exists processing_duration_ms integer"
    ),
}
OUTBOX_COMPAT_COLUMNS = {
    "lease_id": "alter table outbox add column if not exists lease_id text",
    "leased_until": "alter table outbox add column if not exists leased_until timestamptz",
    "schema_version": (
        "alter table outbox add column if not exists schema_version integer not null default 1"
    ),
}
OUTBOX_CLAIM_INDEX = (
    "create index if not exists ix_outbox_claim on outbox (source, sent_at, leased_until, id)"
)
OUTBOX_CLAIM_ALL_SOURCES_INDEX = (
    "create index if not exists ix_outbox_claim_all_sources "
    "on outbox (sent_at, leased_until, id) where sent_at is null"
)
USER_ACCOUNT_COMPAT_COLUMNS = {
    "email": "alter table user_accounts add column if not exists email text",
    "password_hash": "alter table user_accounts add column if not exists password_hash text",
    "role": "alter table user_accounts add column if not exists role text",
}
USER_ACCOUNT_ROLE_BACKFILL = f"""
with ranked as (
    select user_id,
           row_number() over (order by created_at, user_id) as row_number
    from user_accounts
    where role is null
)
update user_accounts as users
set role = case
    when ranked.row_number = 1 then '{ServiceRole.SERVICE_ADMIN.value}'
    else '{ServiceRole.USER.value}'
end
from ranked
where users.user_id = ranked.user_id
"""
USER_ACCOUNT_ROLE_DEFAULT = (
    f"alter table user_accounts alter column role set default '{ServiceRole.USER.value}'"
)
USER_ACCOUNT_ROLE_NOT_NULL = "alter table user_accounts alter column role set not null"
WORKSPACE_COMPAT_COLUMNS = {
    "agent_commands": {
        "workspace_id": "alter table agent_commands add column if not exists workspace_id text",
    },
    "evidence": {
        "workspace_id": "alter table evidence add column if not exists workspace_id text",
    },
    "rca_reports": {
        "workspace_id": "alter table rca_reports add column if not exists workspace_id text",
    },
    # 일반 테이블 컬럼 호환(워크스페이스 한정 아님) — per-cluster agent 토큰 해시 추가.
    # backfill 대상 아님(NULL = 미인증 → 재등록 시 채워짐).
    "cluster_registrations": {
        "agent_token_hash": (
            "alter table cluster_registrations add column if not exists agent_token_hash text"
        ),
        "agent_envelope_public_key": (
            "alter table cluster_registrations add column if not exists "
            "agent_envelope_public_key text"
        ),
        "agent_envelope_private_key_encrypted": (
            "alter table cluster_registrations add column if not exists "
            "agent_envelope_private_key_encrypted text"
        ),
    },
}
WORKSPACE_BACKFILL_COLUMNS = (
    "agent_commands",
    "evidence",
    "rca_reports",
)
USER_ACCOUNT_EMAIL_INDEX = (
    "create unique index if not exists ux_user_accounts_email "
    "on user_accounts (email) where email is not null"
)
CLUSTER_AGENT_TOKEN_HASH_INDEX = (
    "create index if not exists ix_cluster_registrations_agent_token_hash "
    "on cluster_registrations (agent_token_hash) where agent_token_hash is not null"
)
OPERATIONAL_INDEXES = (
    (
        "create index if not exists ix_agent_commands_available "
        "on agent_commands (workspace_id, cluster_id, status, priority desc, created_at) "
        f"where status in ('{CommandStatus.QUEUED}', '{CommandStatus.LEASED}', "
        f"'{CommandStatus.RUNNING}')"
    ),
    ("create index if not exists ix_event_processing_status on event_processing (status)"),
    (
        "create index if not exists ix_event_dead_letters_open "
        f"on event_dead_letters (status, id) where status = '{DEAD_LETTER_STATUS_OPEN}'"
    ),
    (
        "create index if not exists ix_events_correlation_created "
        "on events (correlation_id, created_at)"
    ),
    (
        "create index if not exists ix_agent_command_attempts_due "
        "on agent_command_attempts (workspace_id, cluster_id, status, available_at)"
    ),
    (
        "create index if not exists ix_agent_command_attempts_command "
        "on agent_command_attempts (workspace_id, command_id, attempt_no)"
    ),
    (
        "create index if not exists ix_command_control_actions_command "
        "on command_control_actions (workspace_id, command_id, created_at)"
    ),
)
INVENTORY_FILTER_COMPAT_COLUMNS = {
    "change_ledger_epoch": (
        "alter table inventory_filter_revisions add column if not exists change_ledger_epoch text"
    ),
}
INVENTORY_CHANGE_TIMELINE_COMPAT_INDEXES = (
    (
        "create index if not exists ix_timeline_events_inventory_changes "
        "on timeline_events (workspace_id, cluster_id, occurred_at, event_id) "
        "where source = 'inventory' and activity = 'change' "
        "and event_type in ('add', 'update', 'delete')"
    ),
    (
        "create index if not exists ix_inventory_filter_revisions_change_coverage "
        "on inventory_filter_revisions "
        "(workspace_id, cluster_id, change_ledger_epoch, resources_complete, "
        "observed_at, revision_id)"
    ),
)
REPO_CHANGE_COMPAT_COLUMNS = {
    "workspace_id": "alter table repo_changes add column if not exists workspace_id text",
    "repository_id": "alter table repo_changes add column if not exists repository_id text",
    "watch_target_id": "alter table repo_changes add column if not exists watch_target_id text",
    "binding_id": "alter table repo_changes add column if not exists binding_id text",
    "manifest_path": "alter table repo_changes add column if not exists manifest_path text",
}
MANIFEST_ARTIFACT_COMPAT_COLUMNS = {
    "workspace_id": "alter table manifest_artifacts add column if not exists workspace_id text",
}
MANIFEST_ARTIFACT_DROP_LEGACY_UNIQUE = """
do $$
declare
    old_constraint text;
begin
    select con.conname
    into old_constraint
    from pg_constraint con
    join pg_class rel on rel.oid = con.conrelid
    join pg_namespace nsp on nsp.oid = rel.relnamespace
    where nsp.nspname = current_schema()
      and rel.relname = 'manifest_artifacts'
      and con.contype = 'u'
      and (
          select array_agg(att.attname::text order by ord.ordinality)
          from unnest(con.conkey) with ordinality as ord(attnum, ordinality)
          join pg_attribute att on att.attrelid = rel.oid and att.attnum = ord.attnum
      ) = array['binding_id', 'commit_sha', 'manifest_path']::text[]
    limit 1;

    if old_constraint is not null then
        execute format('alter table manifest_artifacts drop constraint %I', old_constraint);
    end if;
end $$;
"""
MANIFEST_ARTIFACT_WORKSPACE_UNIQUE = """
create unique index if not exists ux_manifest_artifacts_workspace_binding_commit_path
on manifest_artifacts (workspace_id, binding_id, commit_sha, manifest_path)
"""
ROLE_PERMISSION_COMPAT_COLUMNS = {
    "organization_id": "alter table role_permissions add column if not exists organization_id text",
}
ROLE_PERMISSION_SCOPE_BACKFILL = """
update role_permissions
set organization_id = :organization_id
where organization_id is null
"""
ROLE_PERMISSION_SCOPE_DEFAULT = (
    "alter table role_permissions alter column organization_id "
    f"set default '{GLOBAL_ROLE_POLICY_ORGANIZATION_ID}'"
)
ROLE_PERMISSION_SCOPE_NOT_NULL = (
    "alter table role_permissions alter column organization_id set not null"
)
ROLE_PERMISSION_DROP_LEGACY_UNIQUE = """
do $$
declare
    old_constraint text;
begin
    select con.conname
    into old_constraint
    from pg_constraint con
    join pg_class rel on rel.oid = con.conrelid
    join pg_namespace nsp on nsp.oid = rel.relnamespace
    where nsp.nspname = current_schema()
      and rel.relname = 'role_permissions'
      and con.contype = 'u'
      and (
          select array_agg(att.attname::text order by ord.ordinality)
          from unnest(con.conkey) with ordinality as ord(attnum, ordinality)
          join pg_attribute att on att.attrelid = rel.oid and att.attnum = ord.attnum
      ) = array['resource_type', 'role', 'permission']::text[]
    limit 1;

    if old_constraint is not null then
        execute format('alter table role_permissions drop constraint %I', old_constraint);
    end if;
end $$;
"""
ROLE_PERMISSION_SCOPED_UNIQUE = """
create unique index if not exists ux_role_permissions_organization_resource_role_permission
on role_permissions (organization_id, resource_type, role, permission)
"""
MEMBER_RESOURCE_ROLE_MIGRATE_LEGACY_ROLES = f"""
update member_resource_roles
set role = case role
    when 'owner' then '{ResourceRole.CLUSTER_STEWARD.value}'
    when 'admin' then '{ResourceRole.CLUSTER_STEWARD.value}'
    when 'maintainer' then '{ResourceRole.INCIDENT_OPERATOR.value}'
    when 'deployer' then '{ResourceRole.RELEASE_OPERATOR.value}'
    when 'developer' then '{ResourceRole.RELEASE_OPERATOR.value}'
    when 'viewer' then '{ResourceRole.OBSERVER.value}'
    else role
end,
updated_at = now()
where role in ('owner', 'admin', 'maintainer', 'deployer', 'developer', 'viewer')
"""
ROLE_PERMISSION_DELETE_LEGACY_ALIAS_DUPLICATES = f"""
with mapped as (
    select
        id,
        organization_id,
        resource_type,
        case role
            when 'owner' then '{ResourceRole.CLUSTER_STEWARD.value}'
            when 'admin' then '{ResourceRole.CLUSTER_STEWARD.value}'
            when 'maintainer' then '{ResourceRole.INCIDENT_OPERATOR.value}'
            when 'deployer' then '{ResourceRole.RELEASE_OPERATOR.value}'
            when 'developer' then '{ResourceRole.RELEASE_OPERATOR.value}'
            when 'viewer' then '{ResourceRole.OBSERVER.value}'
            else role
        end as migrated_role,
        case permission
            when 'read' then '{Permission.CLUSTER_READ.value}'
            when 'write' then '{Permission.CONFIG_UPDATE.value}'
            when 'deploy' then '{Permission.DEPLOY_RUN.value}'
            when 'admin' then '{Permission.CLUSTER_ROLE_MANAGE.value}'
            else permission
        end as migrated_permission
    from role_permissions
    where role in ('owner', 'admin', 'maintainer', 'deployer', 'developer', 'viewer')
       or permission in ('read', 'write', 'deploy', 'admin')
),
ranked as (
    select
        id,
        row_number() over (
            partition by organization_id, resource_type, migrated_role, migrated_permission
            order by id
        ) as duplicate_rank
    from mapped
),
duplicates as (
    select id from ranked where duplicate_rank > 1
    union
    select mapped.id
    from mapped
    join role_permissions existing
      on existing.id <> mapped.id
     and existing.organization_id = mapped.organization_id
     and existing.resource_type = mapped.resource_type
     and existing.role = mapped.migrated_role
     and existing.permission = mapped.migrated_permission
    where existing.role not in ('owner', 'admin', 'maintainer', 'deployer', 'developer', 'viewer')
      and existing.permission not in ('read', 'write', 'deploy', 'admin')
)
delete from role_permissions
where id in (select id from duplicates);
"""
ROLE_PERMISSION_MIGRATE_LEGACY_ALIASES = f"""
update role_permissions
set role = case role
    when 'owner' then '{ResourceRole.CLUSTER_STEWARD.value}'
    when 'admin' then '{ResourceRole.CLUSTER_STEWARD.value}'
    when 'maintainer' then '{ResourceRole.INCIDENT_OPERATOR.value}'
    when 'deployer' then '{ResourceRole.RELEASE_OPERATOR.value}'
    when 'developer' then '{ResourceRole.RELEASE_OPERATOR.value}'
    when 'viewer' then '{ResourceRole.OBSERVER.value}'
    else role
end,
permission = case permission
    when 'read' then '{Permission.CLUSTER_READ.value}'
    when 'write' then '{Permission.CONFIG_UPDATE.value}'
    when 'deploy' then '{Permission.DEPLOY_RUN.value}'
    when 'admin' then '{Permission.CLUSTER_ROLE_MANAGE.value}'
    else permission
end,
updated_at = now()
where role in ('owner', 'admin', 'maintainer', 'deployer', 'developer', 'viewer')
   or permission in ('read', 'write', 'deploy', 'admin')
"""

# 풀 제어: 앱은 PgBouncer 로 연결(저렴). pre_ping 으로 죽은 연결은 쓰기 전에 폐기,
# timeout 으로 하트비트 창(30s) 안에 빨리 실패.
# 게이트웨이(전 트래픽 + long-poll)와 워커(배치)의 트래픽 특성이 달라
# 풀 크기·대기 한도는 서비스별 deploy env 로 오버라이드 가능(기본값 불변).
DB_POOL_SIZE_ENV = "DB_POOL_SIZE"  # 풀 상주 커넥션 수(기본 2)
DB_MAX_OVERFLOW_ENV = "DB_MAX_OVERFLOW"  # 순간 초과 허용 커넥션 수(기본 2)
DB_POOL_TIMEOUT_ENV = "DB_POOL_TIMEOUT_SECONDS"  # 풀 커넥션 대기 한도 초(기본 10)
POOL_OPTIONS = {
    "pool_size": int(env(DB_POOL_SIZE_ENV, "2")),
    "max_overflow": int(env(DB_MAX_OVERFLOW_ENV, "2")),
    "pool_timeout": int(env(DB_POOL_TIMEOUT_ENV, "10")),
    "pool_pre_ping": True,
    "pool_recycle": 300,
}


def connect_args_for(sqlalchemy_url: str) -> dict[str, object]:
    """드라이버별 connect_args — 드라이버 전용 옵션을 타 드라이버에 전달하지 않음.

    psycopg 한정 prepare_threshold=None: PgBouncer transaction pooling 에서
    prepared statement 가 트랜잭션을 가로질러 깨지지 않도록 비활성화.
    SQLite 등 다른 드라이버는 이 옵션을 모르는 인자로 거부하므로 분기 필수.
    """
    if sqlalchemy_url.startswith("postgresql+psycopg"):
        return {"prepare_threshold": None}
    return {}


def compact_error(error: str) -> str:
    return error[:ERROR_MESSAGE_LIMIT]


def iso_or_none(value: object) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def serialize_dead_letter(row: JsonObject) -> JsonObject:
    item = dict(row)
    item["created_at"] = iso_or_none(item.get("created_at"))
    item["replayed_at"] = iso_or_none(item.get("replayed_at"))
    return item


def serialize_command(row: JsonObject) -> JsonObject:
    item = dict(row)
    item["leased_until"] = iso_or_none(item.get("leased_until"))
    return item


def row_dict(row: Any) -> JsonObject:
    return dict(row)


_ACTIVE_CONN: ContextVar[Connection | None] = ContextVar("active_conn", default=None)


def has_active_connection() -> bool:
    return _ACTIVE_CONN.get() is not None


def unit_of_work_or_null(db: Any) -> AbstractContextManager[Any]:
    """db 가 unit_of_work 를 제공하면 그 트랜잭션을, 아니면 no-op 컨텍스트를 반환함.

    라우터가 여러 저장소 쓰기와 outbox 스테이징을 한 트랜잭션으로 묶는 용도 —
    안쪽의 connection()/unit_of_work() 호출과 accept_body 의 outbox 스테이징이
    같은 스레드에서 이 트랜잭션에 합류함.
    unit_of_work 가 없는 테스트 비실데이터는 기존처럼 개별 호출로 동작함.
    """
    unit_of_work = getattr(db, "unit_of_work", None)
    if callable(unit_of_work):
        return unit_of_work()
    return nullcontext()


def configure_transaction(conn: Connection) -> None:
    conn.execute(text(f"set local lock_timeout = '{DB_LOCK_TIMEOUT}'"))
    conn.execute(text(f"set local statement_timeout = '{DB_STATEMENT_TIMEOUT}'"))
    conn.execute(
        text(f"set local idle_in_transaction_session_timeout = '{DB_IDLE_IN_TRANSACTION_TIMEOUT}'")
    )


def acquire_schema_init_lock(conn: Connection) -> None:
    conn.execute(text(f"set local lock_timeout = '{DB_SCHEMA_INIT_LOCK_TIMEOUT}'"))
    conn.execute(text(f"set local statement_timeout = '{DB_SCHEMA_INIT_STATEMENT_TIMEOUT}'"))
    conn.execute(
        text("select pg_advisory_xact_lock(:namespace, :key)"),
        {"namespace": SCHEMA_INIT_LOCK_NAMESPACE, "key": SCHEMA_INIT_LOCK_KEY},
    )


def schema_compatibility_issues(
    expected: dict[str, set[str]],
    actual: dict[str, set[str]],
) -> list[str]:
    """읽기 전용 schema 검증 결과 — 누락 table/column을 결정적으로 정렬한다."""
    issues: list[str] = []
    for table_name in sorted(expected):
        if table_name not in actual:
            issues.append(f"table:{table_name}")
            continue
        for column_name in sorted(expected[table_name] - actual[table_name]):
            issues.append(f"column:{table_name}.{column_name}")
    return issues


async def configure_async_transaction(conn: Any) -> None:
    await conn.execute(text(f"set local lock_timeout = '{DB_LOCK_TIMEOUT}'"))
    await conn.execute(text(f"set local statement_timeout = '{DB_STATEMENT_TIMEOUT}'"))
    await conn.execute(
        text(f"set local idle_in_transaction_session_timeout = '{DB_IDLE_IN_TRANSACTION_TIMEOUT}'")
    )


def ensure_required_postgres_extensions(conn: Connection) -> None:
    """Install metadata index extensions before initialize-mode ``create_all``.

    Production startup runs in verify mode and remains read-only; Alembic owns the same
    extension creation there.
    """
    if conn.dialect.name != "postgresql":
        return
    for extension in POSTGRES_REQUIRED_EXTENSIONS:
        conn.execute(text(f"CREATE EXTENSION IF NOT EXISTS {extension}"))


class DatabaseConnection:
    def __init__(self) -> None:
        self.url = required_env(DATABASE_URL_ENV)
        connect_args = connect_args_for(self.sqlalchemy_url)
        self.engine: Engine = create_engine(
            self.sqlalchemy_url, connect_args=connect_args, **POOL_OPTIONS
        )
        self.async_engine: AsyncEngine = create_async_engine(
            self.sqlalchemy_url, connect_args=connect_args, **POOL_OPTIONS
        )

    @property
    def sqlalchemy_url(self) -> str:
        return self.url.replace("postgresql://", "postgresql+psycopg://", 1)

    @contextmanager
    def connection(self):
        active = _ACTIVE_CONN.get()
        if active is not None:
            yield active  # UoW 트랜잭션에 합류(commit 은 UoW 소유)
            return
        with self.engine.begin() as conn:
            configure_transaction(conn)
            yield conn

    @contextmanager
    def unit_of_work(self):
        """한 트랜잭션 — 안에서 connection() 호출은 모두 이 커넥션을 사용.

        이미 활성 UoW 안이면 새 트랜잭션을 열지 않고 합류 — 중첩 호출이
        바깥 트랜잭션보다 먼저 커밋되는 원자성 파괴 방지(commit 은 최상위 UoW 소유).
        """
        active = _ACTIVE_CONN.get()
        if active is not None:
            yield active
            return
        with self.engine.begin() as conn:
            configure_transaction(conn)
            token = _ACTIVE_CONN.set(conn)
            try:
                yield conn
            finally:
                _ACTIVE_CONN.reset(token)

    @asynccontextmanager
    async def async_connection(self):
        async with self.async_engine.begin() as conn:
            await configure_async_transaction(conn)
            yield conn

    def check_ready(self) -> None:
        """가벼운 연결 확인(SELECT 1) — readiness 프로브용. DDL/마이그레이션 안 함."""
        with self.connection() as conn:
            conn.execute(text("SELECT 1"))

    def verify_schema(self) -> None:
        """현재 metadata의 table/column이 모두 있는지 읽기 전용으로 검증한다."""
        from domains.registry import load_domain_tables

        load_domain_tables()
        expected = {
            table.name: {column.name for column in table.columns}
            for table in metadata.sorted_tables
        }
        with self.connection() as conn:
            rows = conn.execute(
                text(
                    "select table_name, column_name "
                    "from information_schema.columns "
                    "where table_schema = current_schema()"
                )
            ).mappings()
            actual: dict[str, set[str]] = {}
            for row in rows:
                actual.setdefault(str(row["table_name"]), set()).add(str(row["column_name"]))
        issues = schema_compatibility_issues(expected, actual)
        if not issues:
            return
        preview = ", ".join(issues[:20])
        suffix = f" (+{len(issues) - 20} more)" if len(issues) > 20 else ""
        raise RuntimeError(f"database schema verification failed: {preview}{suffix}")

    def init(self) -> None:
        from domains.registry import load_domain_tables

        load_domain_tables()  # domains/*/models.py 자동 등록(create_all 전)
        with self.engine.begin() as conn:
            configure_transaction(conn)
            acquire_schema_init_lock(conn)
            token = _ACTIVE_CONN.set(conn)
            try:
                ensure_required_postgres_extensions(conn)
                metadata.create_all(conn)
                self.ensure_compatible_schema(conn)
                ensure_default_workspace = getattr(self, "ensure_default_workspace", None)
                if callable(ensure_default_workspace):
                    ensure_default_workspace()
                ensure_default_organization = getattr(self, "ensure_default_organization", None)
                if callable(ensure_default_organization):
                    ensure_default_organization()
                ensure_default_role_permissions = getattr(
                    self,
                    "ensure_default_role_permissions",
                    None,
                )
                if callable(ensure_default_role_permissions):
                    ensure_default_role_permissions()
            finally:
                _ACTIVE_CONN.reset(token)

    def ensure_compatible_schema(self, existing_conn: Connection | None = None) -> None:
        """실제 마이그레이션 도구 도입 전까지 로컬 개발 DB 를 호환 상태로 유지"""
        if existing_conn is not None:
            self._apply_compatible_schema(existing_conn)
            return

        with self.engine.begin() as conn:
            configure_transaction(conn)
            acquire_schema_init_lock(conn)
            self._apply_compatible_schema(conn)

    def _apply_compatible_schema(self, conn: Connection) -> None:
        self._add_missing_columns(conn, "events", EVENT_COMPAT_COLUMNS)
        self._add_missing_columns(conn, "event_processing", EVENT_PROCESSING_COMPAT_COLUMNS)
        self._add_missing_columns(conn, "outbox", OUTBOX_COMPAT_COLUMNS)
        conn.execute(text(OUTBOX_CLAIM_INDEX))
        conn.execute(text(OUTBOX_CLAIM_ALL_SOURCES_INDEX))

        self._add_missing_columns(conn, "agent_commands", AGENT_COMMAND_COMPAT_COLUMNS)
        self._add_missing_columns(conn, "user_accounts", USER_ACCOUNT_COMPAT_COLUMNS)
        conn.execute(text(USER_ACCOUNT_ROLE_BACKFILL))
        conn.execute(text(USER_ACCOUNT_ROLE_DEFAULT))
        conn.execute(text(USER_ACCOUNT_ROLE_NOT_NULL))
        conn.execute(text(USER_ACCOUNT_EMAIL_INDEX))

        self._add_missing_columns(conn, "repo_changes", REPO_CHANGE_COMPAT_COLUMNS)
        conn.execute(
            text(
                """
                update repo_changes
                set workspace_id = :workspace_id
                where workspace_id is null
                """
            ),
            {"workspace_id": DEFAULT_WORKSPACE_ID},
        )
        conn.execute(
            text(
                f"""
                alter table repo_changes
                alter column workspace_id set default '{DEFAULT_WORKSPACE_ID}'
                """
            )
        )
        conn.execute(text("alter table repo_changes alter column workspace_id set not null"))

        self._add_missing_columns(conn, "manifest_artifacts", MANIFEST_ARTIFACT_COMPAT_COLUMNS)
        conn.execute(
            text(
                """
                update manifest_artifacts
                set workspace_id = :workspace_id
                where workspace_id is null
                """
            ),
            {"workspace_id": DEFAULT_WORKSPACE_ID},
        )
        conn.execute(
            text(
                f"""
                alter table manifest_artifacts
                alter column workspace_id set default '{DEFAULT_WORKSPACE_ID}'
                """
            )
        )
        conn.execute(text("alter table manifest_artifacts alter column workspace_id set not null"))
        conn.execute(text(MANIFEST_ARTIFACT_DROP_LEGACY_UNIQUE))
        conn.execute(text(MANIFEST_ARTIFACT_WORKSPACE_UNIQUE))
        self._add_missing_columns(conn, "role_permissions", ROLE_PERMISSION_COMPAT_COLUMNS)
        conn.execute(
            text(ROLE_PERMISSION_SCOPE_BACKFILL),
            {"organization_id": GLOBAL_ROLE_POLICY_ORGANIZATION_ID},
        )
        conn.execute(text(ROLE_PERMISSION_SCOPE_DEFAULT))
        conn.execute(text(ROLE_PERMISSION_SCOPE_NOT_NULL))
        conn.execute(text(ROLE_PERMISSION_DROP_LEGACY_UNIQUE))
        conn.execute(text(ROLE_PERMISSION_SCOPED_UNIQUE))
        conn.execute(text(MEMBER_RESOURCE_ROLE_MIGRATE_LEGACY_ROLES))
        conn.execute(text(ROLE_PERMISSION_DELETE_LEGACY_ALIAS_DUPLICATES))
        conn.execute(text(ROLE_PERMISSION_MIGRATE_LEGACY_ALIASES))

        for table_name, columns in WORKSPACE_COMPAT_COLUMNS.items():
            self._add_missing_columns(conn, table_name, columns)
        conn.execute(text(CLUSTER_AGENT_TOKEN_HASH_INDEX))
        for statement in OPERATIONAL_INDEXES:
            conn.execute(text(statement))
        self._add_missing_columns(
            conn,
            "inventory_filter_revisions",
            INVENTORY_FILTER_COMPAT_COLUMNS,
        )
        for statement in INVENTORY_CHANGE_TIMELINE_COMPAT_INDEXES:
            conn.execute(text(statement))

        for table_name in WORKSPACE_BACKFILL_COLUMNS:
            conn.execute(
                text(
                    f"""
                    update {table_name}
                    set workspace_id = :workspace_id
                    where workspace_id is null
                    """
                ),
                {"workspace_id": DEFAULT_WORKSPACE_ID},
            )
            conn.execute(
                text(
                    f"""
                    alter table {table_name}
                    alter column workspace_id set default '{DEFAULT_WORKSPACE_ID}'
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    alter table {table_name}
                    alter column workspace_id set not null
                    """
                )
            )

    def _add_missing_columns(
        self, conn: Connection, table_name: str, columns: dict[str, str]
    ) -> None:
        existing_columns = self._existing_columns(conn, table_name)
        for column, statement in columns.items():
            if column in existing_columns:
                continue
            self._execute_schema_ddl(conn, statement)

    @staticmethod
    def _execute_schema_ddl(conn: Connection, statement: str) -> None:
        conn.execute(text(f"set local lock_timeout = '{DB_LOCK_TIMEOUT}'"))
        conn.execute(text(statement))

    @staticmethod
    def _existing_columns(conn: Connection, table_name: str) -> set[str]:
        return set(
            conn.execute(
                text(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = current_schema()
                      and table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            ).scalars()
        )

    def dispose(self) -> None:
        self.engine.dispose()

    async def dispose_async(self) -> None:
        await self.async_engine.dispose()
