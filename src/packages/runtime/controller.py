"""OSS controller composition plan and service entrypoint loader."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from uvicorn import Config, Server

from packages.config.settings import env
from packages.contracts.event_bus.interfaces import (
    EventConsumerMetrics,
    EventEnvelope,
    EventSubscription,
    JsonObject,
)
from packages.events.bus import NatsEventBus
from packages.events.in_memory import InMemoryEventBus
from packages.runtime.app import App
from packages.runtime.discovery import DiscoveredService, discover_services
from packages.runtime.worker import WorkerRuntime

CONTROLLER_EVENT_BUS_MODE_ENV = "CONTROLLER_EVENT_BUS_MODE"
EVENT_BUS_MODES = frozenset({"inprocess", "nats"})
AGENT_SERVICE_NAMES = frozenset({"cluster-agent"})
CORE_CONTROLLER_SERVICE_NAMES = frozenset(
    {
        "analyze-worker",
        "api-gateway",
        "ai-diff-worker",
        "dispatch-worker",
        "evidence-worker",
        "incident-worker",
        "outbox-relay",
        "plan-worker",
        "rca-feedback-worker",
        "rca-worker",
        "recovery-worker",
        "safe-pr-worker",
        "scm-worker",
        "select-worker",
        "dead-letter-monitor",
    }
)
API_GATEWAY_SERVICE_NAME = "api-gateway"
BUS_INJECTABLE_ASYNC_SERVICES = frozenset({"outbox-relay"})
HTTP_SHUTDOWN_TIMEOUT_SECONDS = 10.0


def _choice_env(name: str, default: str, allowed: frozenset[str]) -> str:
    value = os.getenv(name, default).strip().lower()
    if value not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}: {value!r}")
    return value


@dataclass(frozen=True)
class ControllerProfile:
    event_bus_mode: str = "inprocess"

    def __post_init__(self) -> None:
        if self.event_bus_mode not in EVENT_BUS_MODES:
            raise ValueError(f"invalid controller event bus mode: {self.event_bus_mode!r}")

    @classmethod
    def from_env(cls) -> ControllerProfile:
        return cls(
            event_bus_mode=_choice_env(
                CONTROLLER_EVENT_BUS_MODE_ENV,
                "inprocess",
                EVENT_BUS_MODES,
            ),
        )


@dataclass(frozen=True)
class CompositionPlan:
    event_bus_mode: str
    controller_services: tuple[DiscoveredService, ...]
    agent_services: tuple[DiscoveredService, ...]

    def service_signature(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(
            sorted(
                (
                    service.name,
                    service.kind,
                    "agent" if service in self.agent_services else "controller",
                )
                for service in (*self.controller_services, *self.agent_services)
            )
        )


def validate_event_bus_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized not in EVENT_BUS_MODES:
        raise ValueError(
            f"{CONTROLLER_EVENT_BUS_MODE_ENV} must be one of {sorted(EVENT_BUS_MODES)}: {mode!r}"
        )
    return normalized


def event_bus_for_mode(mode: str) -> InMemoryEventBus | NatsEventBus:
    normalized = validate_event_bus_mode(mode)
    if normalized == "inprocess":
        return InMemoryEventBus()
    return NatsEventBus()


def build_composition_plan(
    root: Path,
    *,
    event_bus_mode: str | None = None,
) -> CompositionPlan:
    mode = event_bus_mode or ControllerProfile.from_env().event_bus_mode
    validate_event_bus_mode(mode)
    services = discover_services(root)
    agent = tuple(service for service in services if service.name in AGENT_SERVICE_NAMES)
    controller = tuple(
        service for service in services if service.name in CORE_CONTROLLER_SERVICE_NAMES
    )
    found_agent_names = {service.name for service in agent}
    if found_agent_names != AGENT_SERVICE_NAMES:
        raise ValueError(
            f"agent composition mismatch: expected={sorted(AGENT_SERVICE_NAMES)}, "
            f"actual={sorted(found_agent_names)}"
        )
    found_controller_names = {service.name for service in controller}
    if found_controller_names != CORE_CONTROLLER_SERVICE_NAMES:
        raise ValueError(
            "controller composition mismatch: "
            f"expected={sorted(CORE_CONTROLLER_SERVICE_NAMES)}, "
            f"actual={sorted(found_controller_names)}"
        )
    return CompositionPlan(mode, controller, agent)


@dataclass(frozen=True)
class LoadedControllerService:
    service: DiscoveredService
    module: ModuleType


class BorrowedEventBus:
    """Delegate to a root-owned bus without letting child runtimes close it."""

    def __init__(self, bus: InMemoryEventBus | NatsEventBus) -> None:
        self.bus = bus

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def emit(
        self,
        subject: str,
        source: str,
        payload: JsonObject,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> EventEnvelope:
        return await self.bus.emit(subject, source, payload, correlation_id, causation_id)

    async def publish_envelope(self, evt: EventEnvelope) -> EventEnvelope:
        return await self.bus.publish_envelope(evt)

    async def subscribe(self, subject: str, durable: str) -> EventSubscription:
        return await self.bus.subscribe(subject, durable)

    async def consumer_metrics(self, subject: str, durable: str) -> EventConsumerMetrics:
        return await self.bus.consumer_metrics(subject, durable)


class ControllerRuntime:
    def __init__(self, root: Path, profile: ControllerProfile | None = None) -> None:
        self.root = root
        self.profile = profile or ControllerProfile.from_env()
        self.plan = build_composition_plan(root, event_bus_mode=self.profile.event_bus_mode)
        self.loaded = tuple(
            LoadedControllerService(service, load_service_entrypoint(root, service))
            for service in self.plan.controller_services
        )

    def check_report(self) -> dict[str, Any]:
        counts = {kind: 0 for kind in ("worker", "async", "http")}
        for loaded in self.loaded:
            counts[loaded.service.kind] += 1
            self._validate_entrypoint(loaded)
        return {
            "event_bus_mode": self.profile.event_bus_mode,
            "discovered_services": len(self.plan.controller_services + self.plan.agent_services),
            "controller_services": len(self.plan.controller_services),
            "agent_services": len(self.plan.agent_services),
            "worker_services": counts["worker"],
            "async_services": counts["async"],
            "http_services": counts["http"],
        }

    async def serve(self) -> None:
        self.check_report()
        bus = event_bus_for_mode(self.profile.event_bus_mode)
        borrowed = BorrowedEventBus(bus)
        await bus.connect()
        servers: list[Server] = []
        service_tasks: list[asyncio.Task[Any]] = []
        server_tasks: list[asyncio.Task[Any]] = []
        waiter: asyncio.Task[bool] | None = None
        stopping = asyncio.Event()
        try:
            for loaded in self.loaded:
                if loaded.service.kind == "worker":
                    app = loaded.module.app
                    service_tasks.append(
                        asyncio.create_task(
                            WorkerRuntime(app.handler_spec(), bus=borrowed).run(),
                            name=loaded.service.name,
                        )
                    )
                elif loaded.service.kind == "async":
                    runner = loaded.module.run
                    args = (
                        (borrowed,) if loaded.service.name in BUS_INJECTABLE_ASYNC_SERVICES else ()
                    )
                    service_tasks.append(
                        asyncio.create_task(runner(*args), name=loaded.service.name)
                    )
                else:
                    server = self._http_server(loaded, borrowed)
                    servers.append(server)
                    server_tasks.append(
                        asyncio.create_task(server.serve(), name=loaded.service.name)
                    )
            await asyncio.sleep(0)
            loop = asyncio.get_running_loop()
            for item in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(item, stopping.set)
            waiter = asyncio.create_task(stopping.wait(), name="controller-stop")
            done, _pending = await asyncio.wait(
                [*service_tasks, *server_tasks, waiter],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if waiter not in done:
                stopped = next(task for task in done if task is not waiter)
                error = stopped.exception()
                if error is not None:
                    raise error
                raise RuntimeError(f"controller service stopped unexpectedly: {stopped.get_name()}")
        finally:
            await self._shutdown(servers, service_tasks, server_tasks, waiter)
            await bus.close()

    @staticmethod
    async def _shutdown(
        servers: list[Server],
        service_tasks: list[asyncio.Task[Any]],
        server_tasks: list[asyncio.Task[Any]],
        waiter: asyncio.Task[bool] | None,
    ) -> None:
        for server in servers:
            server.should_exit = True
        if waiter is not None:
            waiter.cancel()
        for task in service_tasks:
            task.cancel()
        cleanup_tasks: list[asyncio.Task[Any]] = [*service_tasks]
        if waiter is not None:
            cleanup_tasks.append(waiter)
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        if not server_tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*server_tasks, return_exceptions=True),
                timeout=HTTP_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            for task in server_tasks:
                task.cancel()
            await asyncio.gather(*server_tasks, return_exceptions=True)

    @staticmethod
    def _validate_entrypoint(loaded: LoadedControllerService) -> None:
        if loaded.service.kind == "worker":
            app = getattr(loaded.module, "app", None)
            if not isinstance(app, App) or app.name != loaded.service.name:
                raise TypeError(f"{loaded.service.path}: invalid worker App")
            app.handler_spec()
            return
        symbol = "run" if loaded.service.kind == "async" else "create_app"
        value = getattr(loaded.module, symbol, None)
        if not callable(value):
            raise TypeError(f"{loaded.service.path}: missing callable {symbol}")

    @staticmethod
    def _http_server(
        loaded: LoadedControllerService,
        bus: BorrowedEventBus,
    ) -> Server:
        if loaded.service.name == API_GATEWAY_SERVICE_NAME:
            # API 세션 권한은 RedisSessionStore의 fail-closed lifecycle만 사용한다.
            app = loaded.module.create_app(event_bus=bus)
            port = int(env("PORT", "8000"))
        else:
            raise ValueError(f"unsupported controller HTTP service: {loaded.service.name}")
        # API gateway가 경로를 redaction한 구조화 요청 로그를 남긴다. Uvicorn의 원문
        # access log를 함께 켜면 /install/<credential> request line이 다시 노출된다.
        server = Server(Config(app, host="0.0.0.0", port=port, log_level="info", access_log=False))
        server.install_signal_handlers = lambda: None
        return server


def load_service_entrypoint(root: Path, service: DiscoveredService) -> ModuleType:
    path = root / service.path
    module_name = f"oss_controller_{service.group}_{service.dirname.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load service entrypoint: {path}")
    local_names = _service_local_module_names(path.parent)
    previous = {name: sys.modules.pop(name, None) for name in local_names}
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(path.parent))
        for name in local_names:
            sys.modules.pop(name, None)
            if previous[name] is not None:
                sys.modules[name] = previous[name]


def _service_local_module_names(service_dir: Path) -> tuple[str, ...]:
    names = {
        child.stem if child.is_file() else child.name
        for child in service_dir.iterdir()
        if (child.is_file() and child.suffix == ".py")
        or (child.is_dir() and (child / "__init__.py").exists())
    }
    names.discard("app")
    return tuple(sorted(names))
