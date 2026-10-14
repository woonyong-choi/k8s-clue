from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from uvicorn import Config, Server

from packages.config.constants import Runtime
from packages.config.logs import configure_logging
from packages.config.settings import env
from packages.contracts.event_bus.interfaces import EventClient, EventConsumerBus, EventHandler
from packages.runtime.worker import EventHandlerSpec, WorkerRuntime
from packages.storage.database import Database

AsyncRunner = Callable[[], Coroutine[Any, Any, None]]
FastApiFactory = Callable[[], FastAPI]
WorkerHandlerFactory = Callable[[EventClient, Database], EventHandler]

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_LOG_LEVEL = "info"
PORT_ENV = "PORT"


def configure_event_loop_policy() -> None:
    if os.name != "nt":
        return
    policy_factory = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_factory is None:
        return
    asyncio.set_event_loop_policy(policy_factory())


@dataclass(frozen=True)
class AsyncService:
    service_name: str
    runner: AsyncRunner

    def run(self) -> None:
        os.environ.setdefault(Runtime.SERVICE_NAME_ENV, self.service_name)
        configure_logging(self.service_name)
        configure_event_loop_policy()
        asyncio.run(self.runner())


@dataclass(frozen=True)
class FastApiService:
    service_name: str
    app_factory: FastApiFactory
    host: str = DEFAULT_HTTP_HOST
    port_env: str = PORT_ENV
    default_port: str = Runtime.DEFAULT_HTTP_PORT
    log_level: str = DEFAULT_LOG_LEVEL

    def run(self) -> None:
        AsyncService(self.service_name, self.serve).run()

    async def serve(self) -> None:
        await Server(
            Config(
                self.app_factory(),
                host=self.host,
                port=int(env(self.port_env, self.default_port)),
                log_level=self.log_level,
            )
        ).serve()


@dataclass(frozen=True)
class WorkerService:
    service_name: str
    subjects: tuple[str, ...]
    handler_factory: WorkerHandlerFactory
    durable_name: str | None = None
    bus: EventConsumerBus | None = None
    consumers: tuple[tuple[str, str], ...] = ()

    def run(self) -> None:
        AsyncService(self.service_name, self.serve).run()

    async def serve(self) -> None:
        spec = EventHandlerSpec(
            service_name=self.service_name,
            subjects=self.subjects,
            handler_factory=self.handler_factory,
            durable_name=self.durable_name,
            consumers=self.consumers,
        )
        await WorkerRuntime(spec, bus=self.bus).run()
