from __future__ import annotations

import pytest
from sqlalchemy.engine import Engine

from packages.storage.database import Database


def test_database_initializes_without_removed_feature_tables(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    # #2: 삭제된 기능의 테이블 없이 새 DB 초기화와 재실행을 완료해야 한다.
    monkeypatch.setenv("DATABASE_URL", engine.url.render_as_string(hide_password=False))
    database = Database()
    try:
        database.init()
        database.verify_schema()
        database.init()
        database.verify_schema()
    finally:
        database.dispose()
