#!/usr/bin/env python3
"""정합성 검사 SQL 을 실행하고 결과를 출력한다.

검사 질의와 조회 질의를 구분해서 보여준다. 조회 질의는 위반 집합을
반환하지 않으므로 quality_results 에 적재하지 않는다.

개수를 문장에 적지 않는다. 한때 "검사 6종" 이라 적어 두고 8개를 출력했다.
"""
from __future__ import annotations

import os
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from sqlalchemy import create_engine, text  # noqa: E402

from domains.datacatalog.checks import CHECK_FILES, LOOKUP_FILES, load_sql  # noqa: E402

DSN = os.environ.get("CATALOG_DATABASE_URL", "postgresql+psycopg://postgres@127.0.0.1:5433/catalog")
engine = create_engine(DSN, future=True)
date = os.environ.get("CATALOG_DATE", datetime.now(UTC).date().isoformat())
params = {"logical_date": date, "logical_ts": datetime.fromisoformat(f"{date}T03:00:00+00:00")}

with engine.begin() as conn:
    run_id = conn.execute(text("SELECT run_id FROM catalog_collection_runs LIMIT 1")).scalar()
    asset_id = conn.execute(text("SELECT asset_id FROM catalog_data_assets LIMIT 1")).scalar()
    full = {**params, "run_id": run_id, "asset_id": asset_id}

    print(f"=== 검사 {len(CHECK_FILES)}종 ===")
    for name in CHECK_FILES:
        sql = load_sql(name)
        bind = {k: v for k, v in full.items() if f":{k}" in sql}
        rows = conn.execute(text(sql), bind).mappings().all()
        mark = "위반 없음" if not rows else f"위반 {len(rows)}건"
        print(f"  {name:26} {mark}")
        for r in rows[:3]:
            print(f"      {dict(r)}")

    print(f"\n=== 조회 질의 {len(LOOKUP_FILES)}종 ===")
    for name in LOOKUP_FILES:
        sql = load_sql(name)
        bind = {k: v for k, v in full.items() if f":{k}" in sql}
        if ":cluster_id" in sql:
            bind["cluster_id"] = "local"
        rows = conn.execute(text(sql), bind).mappings().all()
        print(f"  {name:26} {len(rows)}행")
