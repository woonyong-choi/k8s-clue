"""카탈로그 조회 API 배선.

router.py 는 엔드포인트만 정의하고 접속을 모른다. 접속을 아는 것은 여기다.
라우터가 커넥션을 직접 만들면 테스트가 항상 진짜 DB 를 요구하고, 그러면
경계 동작(커서·절단·권한)을 검증하는 비용이 올라가 결국 검증하지 않게 된다.

get_connection 오버라이드로 갈아 끼우는 이유가 그것이다.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from .router import get_connection, require_read_scope, router


def build_engine(database_url: str | None = None) -> Engine:
    url = database_url or os.environ.get("CATALOG_DATABASE_URL")
    if not url:
        raise RuntimeError("CATALOG_DATABASE_URL 이 필요합니다")
    # pool_pre_ping — 배치와 API 가 같은 DB 를 쓰고 배치가 오래 잠들었다가
    # 깨어난다. 죽은 커넥션을 그대로 쓰면 첫 요청만 500 이 되고 재현이 어렵다.
    return create_engine(url, pool_pre_ping=True, future=True)


def create_app(
    *,
    engine: Engine | None = None,
    verify_token: Any = None,
    **kwargs: Any,
) -> FastAPI:
    app = FastAPI(
        title="Kyro 카탈로그 조회 API",
        version="0.1.0",
        docs_url="/docs",
        **kwargs,
    )
    app.include_router(router)

    resolved = engine or build_engine()

    def connection() -> Iterator[Connection]:
        with resolved.connect() as conn:
            yield conn

    app.dependency_overrides[get_connection] = connection

    # 토큰 검증기를 주입한다. 없으면 라우터의 기본 검사(Bearer 존재 여부)만 돈다.
    # 실제 서비스에서는 서명·aud·scope 를 확인하는 구현을 여기에 꽂는다.
    if verify_token is not None:
        app.dependency_overrides[require_read_scope] = verify_token

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


app_factory = create_app
