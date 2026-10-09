from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal, Protocol, TypeVar, runtime_checkable

from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

LOGGER = logging.getLogger(__name__)
SESSION_STORE_RECONNECT_INITIAL_SECONDS = 1.0
SESSION_STORE_RECONNECT_MAX_SECONDS = 30.0
_REDIS_TRANSIENT_ERRORS = (
    ConnectionError,
    OSError,
    TimeoutError,
    RedisConnectionError,
    RedisTimeoutError,
)
_Result = TypeVar("_Result")
AuthMode = Literal["password", "trusted_proxy"]


@dataclass(frozen=True)
class AuthSession:
    token: str
    user_id: str
    roles: list[str]
    workspace_id: str
    display_name: str | None = None
    email: str | None = None
    auth_mode: AuthMode = "password"

    def __post_init__(self) -> None:
        if self.auth_mode not in {"password", "trusted_proxy"}:
            raise ValueError("unsupported session authentication mode")


@dataclass(frozen=True)
class RedisSessionStoreConfig:
    url: str
    ttl_seconds: int
    key_prefix: str
    token_bytes: int
    default_roles: tuple[str, ...]
    default_workspace_id: str
    rate_limit_key_prefix: str
    rate_limit: int
    rate_limit_window_seconds: int
    email_verification_key_prefix: str
    email_verification_ttl_seconds: int
    email_verification_token_bytes: int


class RateLimitExceeded(Exception):
    def __init__(self, retry_after_seconds: int | None = None) -> None:
        super().__init__("rate limit exceeded")
        self.retry_after_seconds = retry_after_seconds


class SessionStoreUnavailable(RuntimeError):
    """Session authority is unavailable; callers must fail closed."""


class RedisSessionStoreNotConnected(SessionStoreUnavailable):
    pass


@runtime_checkable
class SessionStore(Protocol):
    @property
    def available(self) -> bool:
        """Whether the authoritative session storage is currently usable."""

    async def start_degraded(self) -> bool:
        """Start reconnecting without creating a substitute session authority."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def create_session(
        self,
        user_id: str,
        roles: list[str] | None = None,
        workspace_id: str | None = None,
        display_name: str | None = None,
        email: str | None = None,
        auth_mode: AuthMode = "password",
    ) -> AuthSession: ...

    async def get_session(self, token: str | None) -> AuthSession | None: ...

    async def touch_session(self, token: str | None) -> bool: ...

    async def delete_session(self, token: str) -> None: ...

    async def check_rate_limit(
        self,
        key: str,
        limit: int | None = None,
        window_seconds: int | None = None,
    ) -> None: ...

    async def check_escalating_rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: int,
        lock_steps_seconds: tuple[int, ...],
        strike_ttl_seconds: int,
    ) -> None: ...

    async def create_email_verification_token(self, user_id: str, email: str) -> str: ...

    async def consume_email_verification_token(
        self, token: str | None
    ) -> dict[str, str] | None: ...


class RedisSessionStore:
    def __init__(self, config: RedisSessionStoreConfig) -> None:
        self.config = config
        self.client: AsyncRedis | None = None
        self._closed = True
        self._connection_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[None] | None = None

    @property
    def available(self) -> bool:
        return self.client is not None

    async def connect(self) -> None:
        """Strict connection for services that require session storage at startup."""
        self._closed = False
        await self._connect()

    async def start_degraded(self) -> bool:
        """Start a fail-closed session authority and reconnect asynchronously if Redis is down."""
        self._closed = False
        try:
            await self._connect()
        except _REDIS_TRANSIENT_ERRORS:
            LOGGER.warning("session_store_redis_connect_failed", exc_info=True)
            self._schedule_reconnect()
            return False
        return True

    async def close(self) -> None:
        self._closed = True
        reconnect_task = self._reconnect_task
        self._reconnect_task = None
        if reconnect_task is not None:
            reconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await reconnect_task
        await self._disconnect_client()

    async def _connect(self) -> None:
        # 타임아웃/헬스체크 없으면 half-open(죽은) 연결에서 명령이 무한 대기함
        # (control-plane 재시작·엔드포인트 churn 후 로그인 setex 가 영원히 멈추던 원인).
        # socket_timeout 으로 응답을 유한하게 끊고, health_check_interval 로 idle 연결을
        # 쓰기 전에 ping 검증, keepalive/retry 로 끊긴 연결을 자동 복구함.
        async with self._connection_lock:
            if self._closed or self.client is not None:
                return
            client = AsyncRedis.from_url(
                self.config.url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                retry_on_timeout=True,
            )
            try:
                await client.ping()
            except _REDIS_TRANSIENT_ERRORS:
                await self._close_client(client)
                raise
            if self._closed:
                await self._close_client(client)
                return
            self.client = client

    def _schedule_reconnect(self) -> None:
        task = self._reconnect_task
        if self._closed or self.client is not None or (task is not None and not task.done()):
            return
        self._reconnect_task = asyncio.create_task(
            self._reconnect(), name="session-store-redis-reconnect"
        )

    async def _reconnect(self) -> None:
        delay = SESSION_STORE_RECONNECT_INITIAL_SECONDS
        try:
            while not self._closed and self.client is None:
                await asyncio.sleep(delay)
                try:
                    await self._connect()
                except _REDIS_TRANSIENT_ERRORS:
                    LOGGER.warning("session_store_redis_reconnect_failed", exc_info=True)
                    delay = min(
                        SESSION_STORE_RECONNECT_MAX_SECONDS,
                        max(SESSION_STORE_RECONNECT_INITIAL_SECONDS, delay * 2),
                    )
                    continue
                if self.client is not None:
                    LOGGER.info("session_store_redis_reconnected")
                    return
        finally:
            if self._reconnect_task is asyncio.current_task():
                self._reconnect_task = None

    async def _disconnect_client(self, expected: AsyncRedis | None = None) -> None:
        async with self._connection_lock:
            if expected is not None and self.client is not expected:
                return
            client = self.client
            self.client = None
        await self._close_client(client)

    @staticmethod
    async def _close_client(client: AsyncRedis | None) -> None:
        close = getattr(client, "aclose", None)
        if callable(close):
            with suppress(*_REDIS_TRANSIENT_ERRORS):
                await close()

    async def _with_client(self, action: Callable[[AsyncRedis], Awaitable[_Result]]) -> _Result:
        client = self._client()
        try:
            return await action(client)
        except _REDIS_TRANSIENT_ERRORS as exc:
            await self._disconnect_client(client)
            self._schedule_reconnect()
            raise SessionStoreUnavailable("session storage unavailable") from exc

    async def create_session(
        self,
        user_id: str,
        roles: list[str] | None = None,
        workspace_id: str | None = None,
        display_name: str | None = None,
        email: str | None = None,
        auth_mode: AuthMode = "password",
    ) -> AuthSession:
        token = secrets.token_urlsafe(self.config.token_bytes)
        session = AuthSession(
            token=token,
            user_id=user_id,
            roles=roles or list(self.config.default_roles),
            workspace_id=workspace_id or self.config.default_workspace_id,
            display_name=display_name,
            email=email,
            auth_mode=auth_mode,
        )
        await self._with_client(
            lambda client: client.setex(
                f"{self.config.key_prefix}:{token}",
                self.config.ttl_seconds,
                json.dumps(
                    {
                        "user_id": session.user_id,
                        "roles": session.roles,
                        "workspace_id": session.workspace_id,
                        "display_name": session.display_name,
                        "email": session.email,
                        "auth_mode": session.auth_mode,
                    }
                ),
            )
        )
        return session

    async def get_session(self, token: str | None) -> AuthSession | None:
        if not token:
            return None
        raw = await self._with_client(
            lambda client: client.get(f"{self.config.key_prefix}:{token}")
        )
        if not raw:
            return None
        payload = json.loads(raw)
        return AuthSession(
            token=token,
            user_id=payload["user_id"],
            roles=list(payload.get("roles", [])),
            workspace_id=payload.get("workspace_id", self.config.default_workspace_id),
            display_name=payload.get("display_name"),
            email=payload.get("email"),
            auth_mode=(
                "trusted_proxy" if payload.get("auth_mode") == "trusted_proxy" else "password"
            ),
        )

    async def touch_session(self, token: str | None) -> bool:
        if not token:
            return False
        return bool(
            await self._with_client(
                lambda client: client.expire(
                    f"{self.config.key_prefix}:{token}",
                    self.config.ttl_seconds,
                )
            )
        )

    async def delete_session(self, token: str) -> None:
        await self._with_client(lambda client: client.delete(f"{self.config.key_prefix}:{token}"))

    async def check_rate_limit(
        self,
        key: str,
        limit: int | None = None,
        window_seconds: int | None = None,
    ) -> None:
        threshold = limit if limit is not None else self.config.rate_limit
        window = (
            window_seconds if window_seconds is not None else self.config.rate_limit_window_seconds
        )
        redis_key = f"{self.config.rate_limit_key_prefix}:{key}"

        async def check(client: AsyncRedis) -> None:
            count = await client.incr(redis_key)
            if count == 1:
                await client.expire(redis_key, window)
            if count > threshold:
                retry_after = await client.ttl(redis_key)
                raise RateLimitExceeded(max(1, retry_after))

        await self._with_client(check)

    async def check_escalating_rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: int,
        lock_steps_seconds: tuple[int, ...],
        strike_ttl_seconds: int,
    ) -> None:
        lock_key = f"{self.config.rate_limit_key_prefix}:lock:{key}"

        async def check(client: AsyncRedis) -> None:
            retry_after = await client.ttl(lock_key)
            if retry_after > 0:
                raise RateLimitExceeded(retry_after)

            counter_key = f"{self.config.rate_limit_key_prefix}:count:{key}"
            count = await client.incr(counter_key)
            if count == 1:
                await client.expire(counter_key, window_seconds)
            if count <= limit:
                return

            strike_key = f"{self.config.rate_limit_key_prefix}:strike:{key}"
            strike_count = await client.incr(strike_key)
            if strike_count == 1:
                await client.expire(strike_key, strike_ttl_seconds)
            lock_seconds = lock_steps_seconds[min(strike_count, len(lock_steps_seconds)) - 1]
            await client.setex(lock_key, lock_seconds, str(strike_count))
            raise RateLimitExceeded(lock_seconds)

        await self._with_client(check)

    async def create_email_verification_token(self, user_id: str, email: str) -> str:
        token = secrets.token_urlsafe(self.config.email_verification_token_bytes)
        await self._with_client(
            lambda client: client.setex(
                f"{self.config.email_verification_key_prefix}:{token}",
                self.config.email_verification_ttl_seconds,
                json.dumps({"user_id": user_id, "email": email}),
            )
        )
        return token

    async def consume_email_verification_token(self, token: str | None) -> dict[str, str] | None:
        if not token:
            return None
        key = f"{self.config.email_verification_key_prefix}:{token}"
        raw = await self._with_client(lambda client: client.getdel(key))
        if not raw:
            return None
        payload = json.loads(raw)
        return {"user_id": str(payload["user_id"]), "email": str(payload["email"])}

    def _client(self) -> AsyncRedis:
        if self.client is None:
            raise SessionStoreUnavailable("session storage unavailable")
        return self.client
