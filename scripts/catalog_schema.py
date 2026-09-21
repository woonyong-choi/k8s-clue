#!/usr/bin/env python3
"""카탈로그 테이블을 생성하고, 이미 있는 테이블의 컬럼 누락을 알린다.

create_all 은 IF NOT EXISTS 라서 **이미 있는 테이블에 새 컬럼을 붙이지 않는다.**
모델에 컬럼을 추가하고 이 스크립트만 다시 돌리면 아무 일도 일어나지 않고,
배치가 돌기 시작한 뒤에 UndefinedColumn 으로 죽는다. 실제로 그렇게 죽었다.

그래서 만든 뒤에 모델과 실제 컬럼을 대조하고, 어긋나면 어떤 마이그레이션이
필요한지 알린다. 자동으로 ALTER 하지 않는 이유는 운영 DB 에서 위험하기 때문이다.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from sqlalchemy import create_engine, inspect  # noqa: E402

import domains.datacatalog.models  # noqa: E402,F401
from packages.storage.base import Base  # noqa: E402

DSN = os.environ.get("CATALOG_DATABASE_URL", "postgresql+psycopg://postgres@127.0.0.1:5433/catalog")
engine = create_engine(DSN, future=True)
tables = [t for n, t in Base.metadata.tables.items() if n.startswith("catalog_")]

Base.metadata.create_all(engine, tables=tables)

inspector = inspect(engine)
drift: list[str] = []
for table in tables:
    actual = {c["name"] for c in inspector.get_columns(table.name)}
    missing = {c.name for c in table.columns} - actual
    for column in sorted(missing):
        drift.append(f"{table.name}.{column}")

print(f"{len(tables)}개 테이블 준비 완료")
if drift:
    print()
    print("이미 있는 테이블에 모델의 컬럼이 없습니다:")
    for item in drift:
        print(f"  - {item}")
    print()
    print("create_all 은 기존 테이블을 바꾸지 않습니다. 둘 중 하나를 하세요.")
    print("  마이그레이션 적용 :  alembic upgrade head")
    print("  로컬 초기화       :  make catalog-reset")
    raise SystemExit(1)
