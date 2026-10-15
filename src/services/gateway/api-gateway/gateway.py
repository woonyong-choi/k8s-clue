from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import httpx
from auth import PasswordAuthService, SessionAuthService
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from settings import Settings

from domains.gitops.argo_router import router as argocd_router
from domains.gitops.repository_discovery_router import router as repository_discovery_router
from domains.gitops.router import router as gitops_router
from domains.identity.admin_router import router as identity_admin_router
from domains.identity.dependencies import (
    ClusterAgentIdentity,
    require_admin_session,
    require_cluster_agent,
)
from domains.identity.router import router as identity_router
from domains.rca.history_router import router as incident_history_router
from domains.rca.query_router import router as rca_query_router
from domains.rca.router import router as rca_router
from domains.rca_bundle.router import router as rca_bundle_router
from domains.scm.app_router import router as github_app_router
from domains.target.events import AgentConnectedBody
from domains.target.evidence_jobs import EVIDENCE_JOB_STATUS_LEASED, EVIDENCE_JOB_STATUS_QUEUED
from packages.config.constants import Auth
from packages.config.constants import Redis as RedisConfig
from packages.config.logs import CONTEXT_KEY, get_logger
from packages.config.settings import env
from packages.contracts.event_bus.interfaces import EventConsumerBus
from packages.contracts.gateway import routes as gateway_routes
from packages.contracts.gateway.fields import Gateway
from packages.contracts.gateway.requests import AgentConnectRequest
from packages.contracts.gateway.responses import (
    AcceptedResponse,
    DeadLetterReplayResponse,
    DeadLettersResponse,
    HealthResponse,
)
from packages.contracts.identity import DEFAULT_WORKSPACE_ID, ClusterRegistrationStatus, ServiceRole
from packages.events.bus import NatsEventBus
from packages.runtime.async_db import AsyncDb
from packages.runtime.gateway import ApiEventGateway
from packages.runtime.metrics import (
    render_labeled_counter,
    render_labeled_gauge,
    render_multi_labeled_gauge,
    render_prometheus_metrics,
)
from packages.security.trusted_proxy import assert_trusted_proxy_config_safe
from packages.storage.database import Database, wait_for_database
from packages.storage.engine import unit_of_work_or_null
from packages.storage.sessions import (
    RedisSessionStore,
    RedisSessionStoreConfig,
    SessionStore,
    SessionStoreUnavailable,
)
from services.ai.agent.playbooks.cause import registered_cause_profiles
from services.ai.agent.recovery.authority import DatabaseGitOpsAuthorityReadPort
from services.ai.agent.recovery.dispatch import RecoveryActionPreflight

LOGGER = get_logger(__name__)
STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
INSTALL_LOG_PATH_PREFIX = gateway_routes.INSTALL_MANIFEST_PATH.partition("{")[0]
INSTALL_LOG_REDACTED_PATH = f"{INSTALL_LOG_PATH_PREFIX}[REDACTED]"


def elapsed_ms(started_at: float) -> int:
    return max(0, int((time.perf_counter() - started_at) * 1000))


def sanitize_request_log_path(path: str) -> str:
    if path.startswith(INSTALL_LOG_PATH_PREFIX) and len(path) > len(INSTALL_LOG_PATH_PREFIX):
        return INSTALL_LOG_REDACTED_PATH
    return path


def agent_connected_body_from_request(
    payload: AgentConnectRequest,
    identity: ClusterAgentIdentity,
) -> AgentConnectedBody:
    """agent 연결 이벤트는 body cluster_id가 아니라 인증 identity를 권위값으로 사용함."""
    return AgentConnectedBody(
        cluster_id=identity.cluster_id,
        agent_id=payload.agent_id,
        capabilities=payload.capabilities,
        workspace_id=identity.workspace_id,
    )


def render_gateway_metrics(db: Database) -> str:
    """Collect and render one metrics snapshot outside the ASGI event loop.

    The storage contract is synchronous and each aggregate may wait for a
    PostgreSQL connection.  Keeping the complete snapshot behind one thread
    boundary prevents a slow Prometheus scrape from serialising unrelated
    agent long-polls and browser reads on the process event loop.
    """
    scalar_metrics = {
        "event_dead_letters_open_total": db.open_dead_letter_count(),
        "outbox_pending_total": db.outbox_pending_count(),
        "outbox_oldest_age_seconds": db.outbox_oldest_age_seconds(),
        "evidence_job_queue_oldest_age_seconds": db.oldest_evidence_job_age_seconds(
            EVIDENCE_JOB_STATUS_QUEUED
        ),
        "evidence_job_leased_oldest_age_seconds": db.oldest_evidence_job_age_seconds(
            EVIDENCE_JOB_STATUS_LEASED
        ),
        "gitops_workflow_running_total": db.count_running_workflow_runs(DEFAULT_WORKSPACE_ID),
        "gitops_approvals_open_total": db.count_open_workflow_approvals(DEFAULT_WORKSPACE_ID),
    }
    body = render_prometheus_metrics(scalar_metrics)
    body += render_labeled_counter(
        "event_processing_status_total",
        db.event_processing_status_counts(),
        "status",
    )
    body += render_labeled_gauge(
        "event_processing_duration_avg_ms",
        db.event_processing_duration_avg_ms_by_consumer(),
        "consumer",
    )
    body += render_labeled_gauge(
        "event_processing_duration_max_ms",
        db.event_processing_duration_max_ms_by_consumer(),
        "consumer",
    )
    body += render_multi_labeled_gauge(
        "nats_consumer_pending_events",
        db.event_consumer_pending_by_consumer_subject(),
        ("consumer", "subject"),
    )
    body += render_multi_labeled_gauge(
        "nats_consumer_ack_pending_events",
        db.event_consumer_ack_pending_by_consumer_subject(),
        ("consumer", "subject"),
    )
    body += render_multi_labeled_gauge(
        "nats_consumer_redelivered_events",
        db.event_consumer_redelivered_by_consumer_subject(),
        ("consumer", "subject"),
    )
    body += render_labeled_counter(
        "evidence_job_status_total",
        db.evidence_job_status_counts(),
        "status",
    )
    body += render_labeled_counter(
        "gitops_workflow_status_total",
        db.workflow_run_status_counts(DEFAULT_WORKSPACE_ID),
        "status",
    )
    body += render_labeled_counter(
        "gitops_workflow_current_step_total",
        db.workflow_run_current_step_counts(DEFAULT_WORKSPACE_ID),
        "step",
    )
    return body


class ApiGateway:
    def __init__(
        self,
        *,
        event_bus: EventConsumerBus | None = None,
        session_store: SessionStore | None = None,
    ) -> None:
        if session_store is not None and not isinstance(session_store, SessionStore):
            raise TypeError("api gateway requires a fail-closed session lifecycle")
        self.db = Database()
        self.bus = event_bus or NatsEventBus()
        self.events = ApiEventGateway(self.bus, self.db, Settings.SERVICE_NAME)
        self.sessions = session_store or RedisSessionStore(self._session_store_config())
        self._session_store_started = False
        self.auth = SessionAuthService(self.sessions)
        self.password_auth = PasswordAuthService(self.db, self.sessions)
        self.app = FastAPI(
            title=Settings.APP_TITLE,
            version=Settings.APP_VERSION,
            root_path=env(Settings.ROOT_PATH_ENV, Settings.DEFAULT_ROOT_PATH).strip(),
            lifespan=self.lifespan,
        )
        self._configure_cors(self.app)
        self._configure_session_origin_guard(self.app)
        self._configure_request_logging(self.app)
        # 도메인 router 가 Depends 로 가져갈 공유 객체(클로저 대신 DI).
        self.app.state.db = self.db
        self.app.state.events = self.events
        self.app.state.auth = self.auth
        self.app.state.password_auth = self.password_auth
        self.app.state.rca_rule_profiles = registered_cause_profiles()
        self.app.state.recovery_action_preflight = RecoveryActionPreflight(
            DatabaseGitOpsAuthorityReadPort(AsyncDb(self.db)),
            AsyncDb(self.db),
        )
        self.configure_routes()

    @staticmethod
    def _configure_cors(app: FastAPI) -> None:
        # 브라우저 SPA 가 다른 origin(개발 서버)에서 쿠키 인증 요청을 보내려면 CORS 허용 필요.
        # 쿠키 전송(allow_credentials=True) 시 origin 을 "*" 로 둘 수 없어 명시 목록 사용.
        # CORS_ALLOW_ORIGINS(콤마 구분) 미설정 시 로컬 개발 origin 기본 허용.
        raw = env(Settings.CORS_ALLOW_ORIGINS_ENV, Settings.DEFAULT_CORS_ALLOW_ORIGINS)
        origins = [o.strip() for o in raw.split(",") if o.strip()]
        if not origins:
            return
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @staticmethod
    def _configure_session_origin_guard(app: FastAPI) -> None:
        @app.middleware("http")
        async def session_origin_guard(request: Request, call_next):
            if ApiGateway._session_origin_guard_rejects(request):
                return JSONResponse(
                    status_code=403,
                    content={"detail": Settings.CSRF_REJECT_MESSAGE},
                )
            return await call_next(request)

    @staticmethod
    def _configure_request_logging(app: FastAPI) -> None:
        @app.middleware("http")
        async def request_logging(request: Request, call_next):
            started_at = time.perf_counter()
            context = {
                "method": request.method,
                "path": sanitize_request_log_path(request.url.path),
                "client_host": request.client.host if request.client else None,
                "request_correlation_id": request.headers.get(Gateway.CORRELATION_ID),
            }
            try:
                response = await call_next(request)
            except Exception as exc:
                LOGGER.error(
                    "gateway_request_failed",
                    extra={
                        CONTEXT_KEY: {
                            **context,
                            "duration_ms": elapsed_ms(started_at),
                            "exception_type": type(exc).__name__,
                        }
                    },
                    exc_info=exc,
                )
                raise
            LOGGER.info(
                "gateway_request_completed",
                extra={
                    CONTEXT_KEY: {
                        **context,
                        "status_code": response.status_code,
                        "duration_ms": elapsed_ms(started_at),
                    }
                },
            )
            return response

    @staticmethod
    def _session_origin_guard_rejects(request: Request) -> bool:
        if request.method.upper() not in STATE_CHANGING_METHODS:
            return False
        if Auth.SESSION_COOKIE_NAME not in request.cookies:
            return False
        if request.headers.get(Settings.CSRF_INTENT_HEADER) == Settings.CSRF_INTENT_VALUE:
            return False
        return not ApiGateway._has_same_origin_browser_header(request)

    @staticmethod
    def _has_same_origin_browser_header(request: Request) -> bool:
        origin = request.headers.get("origin")
        if origin:
            return ApiGateway._is_allowed_browser_origin(request, origin)
        referer = request.headers.get("referer")
        if referer:
            return ApiGateway._is_allowed_browser_origin(request, referer)
        return False

    @staticmethod
    def _is_allowed_browser_origin(request: Request, value: str) -> bool:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin in ApiGateway._configured_browser_origins():
            return True
        host = request.headers.get("host", "").split(",", 1)[0].strip().lower()
        return bool(host) and parsed.netloc.lower() == host

    @staticmethod
    def _configured_browser_origins() -> set[str]:
        raw = env(Settings.CORS_ALLOW_ORIGINS_ENV, Settings.DEFAULT_CORS_ALLOW_ORIGINS)
        return {origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()}

    @staticmethod
    def _session_store_config() -> RedisSessionStoreConfig:
        return RedisSessionStoreConfig(
            url=env(Settings.REDIS_URL_ENV, RedisConfig.DEFAULT_URL),
            ttl_seconds=int(env(Settings.SESSION_TTL_ENV, Auth.DEFAULT_SESSION_TTL_SECONDS)),
            key_prefix=Settings.SESSION_KEY_PREFIX,
            token_bytes=Settings.SESSION_TOKEN_BYTES,
            default_roles=(ServiceRole.USER.value,),
            default_workspace_id=DEFAULT_WORKSPACE_ID,
            rate_limit_key_prefix=Settings.RATE_LIMIT_KEY_PREFIX,
            rate_limit=Settings.DEFAULT_RATE_LIMIT,
            rate_limit_window_seconds=Settings.RATE_LIMIT_WINDOW_SECONDS,
            email_verification_key_prefix=Settings.EMAIL_VERIFICATION_KEY_PREFIX,
            email_verification_ttl_seconds=Settings.EMAIL_VERIFICATION_TTL_SECONDS,
            email_verification_token_bytes=Settings.EMAIL_VERIFICATION_TOKEN_BYTES,
        )

    @asynccontextmanager
    async def lifespan(self, _app: FastAPI) -> AsyncIterator[None]:
        assert_trusted_proxy_config_safe()
        await wait_for_database(self.db)
        await self._start_session_store()
        await self.bus.connect()
        try:
            yield
        finally:
            await self.bus.close()
            await self.sessions.close()
            await self.db.dispose_async()
            self.db.dispose()

    async def _start_session_store(self) -> None:
        """Keep gateway live when Redis sessions are down, without changing auth authority."""
        await self.sessions.start_degraded()
        self._session_store_started = True

    def _session_store_available(self) -> bool:
        return self._session_store_started and self.sessions.available

    def configure_routes(self) -> None:
        # 라우트는 도메인별로 등록(가독성). 각 그룹은 self 클로저로 events/db/auth 사용.
        app = self.app
        self._register_frontend_proxy(app)
        self._register_health_routes(app)
        app.include_router(identity_router)
        app.include_router(identity_admin_router)
        app.include_router(repository_discovery_router)
        app.include_router(argocd_router)
        app.include_router(github_app_router)
        app.include_router(gitops_router)
        self._register_ingest_routes(app)
        app.include_router(rca_router)
        app.include_router(rca_query_router)
        app.include_router(rca_bundle_router)
        app.include_router(incident_history_router)
        self._register_dead_letter_routes(app)
        self._register_metrics_routes(app)
        self._register_error_handler(app)

    def _register_frontend_proxy(self, app: FastAPI) -> None:
        @app.middleware("http")
        async def frontend_proxy_middleware(request: Request, call_next):
            path = request.scope.get("path", "")
            if path == "/api" or path.startswith("/api/"):
                stripped = path[4:] or "/"
                request.scope["path"] = stripped
                request.scope["raw_path"] = stripped.encode("ascii", errors="ignore")
                return await call_next(request)
            if self._is_frontend_request(request):
                return await self._proxy_console(request)
            return await call_next(request)

    @staticmethod
    def _is_frontend_request(request: Request) -> bool:
        if request.method not in {"GET", "HEAD"}:
            return False
        path = request.url.path
        if path.startswith("/assets/") or path in {"/favicon.ico", "/manifest.webmanifest"}:
            return True
        return "text/html" in request.headers.get("accept", "")

    @staticmethod
    async def _proxy_console(request: Request) -> Response:
        console_origin = env(Settings.CONSOLE_ORIGIN_ENV, Settings.DEFAULT_CONSOLE_ORIGIN).rstrip(
            "/"
        )
        target = f"{console_origin}{request.url.path}"
        if request.url.query:
            target = f"{target}?{request.url.query}"
        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower() not in {"host", "content-length"}
        }
        try:
            async with httpx.AsyncClient(
                follow_redirects=False,
                timeout=Settings.FRONTEND_PROXY_TIMEOUT_SECONDS,
            ) as client:
                upstream = await client.request(request.method, target, headers=headers)
        except httpx.HTTPError as exc:
            LOGGER.warning(
                "frontend_proxy_error",
                extra={CONTEXT_KEY: {"exception_type": type(exc).__name__, "target": target}},
                exc_info=exc,
            )
            return PlainTextResponse("frontend unavailable", status_code=502)

        excluded_headers = {
            "connection",
            "content-encoding",
            "content-length",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        }
        response_headers = {
            key: value
            for key, value in upstream.headers.items()
            if key.lower() not in excluded_headers
        }
        content = b"" if request.method == "HEAD" else upstream.content
        return Response(
            content=content,
            status_code=upstream.status_code,
            headers=response_headers,
            media_type=upstream.headers.get("content-type"),
        )

    def _register_health_routes(self, app: FastAPI) -> None:
        @app.get(gateway_routes.HEALTHZ_PATH, response_model=HealthResponse)
        async def healthz() -> HealthResponse:
            return HealthResponse(status=Gateway.STATUS_OK, service=Settings.SERVICE_NAME)

        @app.get(
            gateway_routes.READYZ_PATH,
            response_model=HealthResponse,
            response_model_exclude_none=True,
        )
        async def readyz() -> HealthResponse:
            # 가벼운 연결 확인만(스키마 보장은 시작 시 lifespan 에서 1회). DDL 실행 없음.
            if not self._session_store_available():
                raise HTTPException(
                    status_code=Settings.SESSION_STORAGE_UNAVAILABLE_STATUS_CODE,
                    detail=Settings.SESSION_STORAGE_UNAVAILABLE_MESSAGE,
                )
            await asyncio.to_thread(self.db.check_ready)
            return HealthResponse(status=Gateway.STATUS_READY)

    def _register_ingest_routes(self, app: FastAPI) -> None:
        @app.post(gateway_routes.AGENT_CONNECT_PATH, response_model=AcceptedResponse)
        async def agent_connect(
            payload: AgentConnectRequest,
            identity: ClusterAgentIdentity = Depends(require_cluster_agent),
        ) -> AcceptedResponse:
            # cluster_id/workspace_id 는 토큰 identity 에서 — body 의 cluster_id 는 신뢰 안 함.
            with unit_of_work_or_null(self.db):
                self.db.save_cluster_agent_status(
                    workspace_id=identity.workspace_id,
                    cluster_id=identity.cluster_id,
                    agent_id=payload.agent_id,
                    capabilities=payload.capabilities,
                    details=payload.details,
                )
                mark_connected = getattr(self.db, "mark_cluster_registration_connected", None)
                if callable(mark_connected):
                    mark_connected(
                        identity.workspace_id,
                        identity.cluster_id,
                    )
                else:
                    registration_getter = getattr(self.db, "get_cluster_registration", None)
                    registration = (
                        registration_getter(identity.workspace_id, identity.cluster_id)
                        if callable(registration_getter)
                        else None
                    )
                    status_updater = getattr(self.db, "update_cluster_registration_status", None)
                    if (
                        callable(status_updater)
                        and str((registration or {}).get("status") or "")
                        != ClusterRegistrationStatus.UNINSTALL_REQUESTED.value
                    ):
                        status_updater(
                            identity.workspace_id,
                            identity.cluster_id,
                            ClusterRegistrationStatus.REGISTERED.value,
                        )
                accepted = await self.events.accept_body(
                    agent_connected_body_from_request(payload, identity)
                )
            return AcceptedResponse(
                accepted=True,
                event_id=accepted.event.event_id,
                correlation_id=accepted.event.correlation_id,
            )

    def _register_dead_letter_routes(self, app: FastAPI) -> None:
        @app.get(gateway_routes.DEAD_LETTERS_PATH, response_model=DeadLettersResponse)
        async def dead_letters(
            request: Request, limit: int = Settings.DEFAULT_DEAD_LETTER_LIMIT
        ) -> DeadLettersResponse:
            # 전 테넌트 실패 이벤트 노출 → admin 만(일반 세션 금지).
            await require_admin_session(request)
            bounded_limit = max(1, min(limit, Settings.MAX_DEAD_LETTER_LIMIT))
            return DeadLettersResponse(dead_letters=self.db.list_dead_letters(bounded_limit))

        @app.post(
            gateway_routes.DEAD_LETTER_REPLAY_PATH,
            response_model=DeadLetterReplayResponse,
        )
        async def replay_dead_letter(
            request: Request, dead_letter_id: int
        ) -> DeadLetterReplayResponse:
            # 임의 이벤트 재발행 → admin 만.
            await require_admin_session(request)
            dead_letter = self.db.get_dead_letter(dead_letter_id)
            if dead_letter is None:
                raise HTTPException(
                    status_code=Settings.NOT_FOUND_STATUS_CODE,
                    detail=Settings.DEAD_LETTER_NOT_FOUND_MESSAGE,
                )
            if dead_letter[Gateway.STATUS] != Gateway.STATUS_OPEN:
                raise HTTPException(
                    status_code=Settings.CONFLICT_STATUS_CODE,
                    detail=Settings.DEAD_LETTER_NOT_OPEN_MESSAGE,
                )

            # replay 이벤트 스테이징과 replay 표시(열린 행만 원자 UPDATE)를 한 트랜잭션으로 —
            # 동시 replay 는 첫 요청만 통과하고, 진 요청의 스테이징은 롤백됨(이중 재발행 방지).
            with unit_of_work_or_null(self.db):
                accepted = await self.events.accept(
                    dead_letter["original_subject"],
                    dead_letter[Gateway.PAYLOAD],
                    dead_letter[Gateway.CORRELATION_ID],
                    dead_letter["original_event_id"],
                )
                if not self.db.mark_dead_letter_replayed(dead_letter_id, accepted.event.event_id):
                    raise HTTPException(
                        status_code=Settings.CONFLICT_STATUS_CODE,
                        detail=Settings.DEAD_LETTER_NOT_OPEN_MESSAGE,
                    )
            return DeadLetterReplayResponse(
                accepted=True,
                dead_letter_id=dead_letter_id,
                replay_event=accepted.event.to_dict(),
            )

    def _register_metrics_routes(self, app: FastAPI) -> None:
        @app.get("/metrics")
        async def metrics(request: Request) -> PlainTextResponse:
            metrics_token = env(Settings.METRICS_TOKEN_ENV, "")
            if not metrics_token:
                raise HTTPException(
                    status_code=503,
                    detail=Settings.METRICS_TOKEN_NOT_CONFIGURED_MESSAGE,
                )
            if metrics_token:
                header = request.headers.get(Settings.AUTHORIZATION_HEADER, "")
                if not secrets.compare_digest(header, f"Bearer {metrics_token}"):
                    raise HTTPException(status_code=401, detail="metrics token required")
            body = await asyncio.to_thread(render_gateway_metrics, self.db)
            return PlainTextResponse(body, media_type="text/plain; version=0.0.4")

    def _register_error_handler(self, app: FastAPI) -> None:
        @app.exception_handler(SessionStoreUnavailable)
        async def session_store_unavailable(
            _request: Request, _exc: SessionStoreUnavailable
        ) -> JSONResponse:
            return JSONResponse(
                status_code=Settings.SESSION_STORAGE_UNAVAILABLE_STATUS_CODE,
                content={"detail": Settings.SESSION_STORAGE_UNAVAILABLE_MESSAGE},
                headers={"Retry-After": "1"},
            )

        @app.exception_handler(Exception)
        async def unhandled(_request: Request, exc: Exception) -> JSONResponse:
            LOGGER.error(
                "gateway_unhandled_error",
                extra={CONTEXT_KEY: {"exception_type": type(exc).__name__}},
                exc_info=exc,
            )
            return JSONResponse(
                status_code=Settings.GATEWAY_ERROR_STATUS_CODE,
                content={Gateway.ERROR: Settings.GATEWAY_ERROR_MESSAGE},
            )


def create_app(
    *,
    event_bus: EventConsumerBus | None = None,
    session_store: SessionStore | None = None,
) -> FastAPI:
    return ApiGateway(event_bus=event_bus, session_store=session_store).app
