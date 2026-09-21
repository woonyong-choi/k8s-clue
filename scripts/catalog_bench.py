#!/usr/bin/env python3
"""검사 SQL 실행시간을 인덱스 유무로 나눠 측정한다.

사용법:
    make catalog-up && make catalog-schema
    PYTHONPATH=src python3 scripts/catalog_bench.py --days 60 --resources 500

부하 데이터는 결정적으로 생성한다. 같은 인자면 같은 행 수가 나온다.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sqlalchemy import create_engine, text  # noqa: E402

from domains.datacatalog.checks import CHECK_FILES, LOOKUP_FILES, load_sql  # noqa: E402

DEFAULT_DSN = os.environ.get(
    "CATALOG_DATABASE_URL", "postgresql+psycopg://postgres@127.0.0.1:5433/catalog"
)

SYNTH_ROWS = """
INSERT INTO catalog_observed_rows (row_id, run_id, asset_id, row_key, observed_at)
SELECT cr.run_id || '__synth__' || g, cr.run_id, da.asset_id, 'res-' || g,
       (cr.logical_date::timestamptz + (g %% 24) * interval '1 hour')
FROM catalog_collection_runs cr
JOIN catalog_data_assets da ON da.source_id = cr.source_id
CROSS JOIN generate_series(1, %(n)s) g
ON CONFLICT DO NOTHING;

INSERT INTO catalog_observed_fields (row_id, run_id, asset_id, field_path, data_type)
SELECT r.row_id, r.run_id, r.asset_id, f.p, 'string'
FROM catalog_observed_rows r
CROSS JOIN (VALUES ('metadata.name'),('metadata.namespace'),('spec.replicas'),
                   ('status.phase'),('metadata.uid')) f(p)
WHERE r.row_id LIKE '%%__synth__%%'
ON CONFLICT DO NOTHING;

INSERT INTO catalog_normalized_evidence
  (evidence_id, asset_id, run_id, cluster_id, source_id, resource_uid,
   collection_status, observed_at, ingested_at)
SELECT cr.run_id || '__ne__' || g, 'ops.normalized_evidence', cr.run_id,
       'cluster-' || (g %% 3), cr.source_id, 'uid-' || g, 'completed',
       (cr.logical_date::timestamptz + (g %% 24) * interval '1 hour'),
       (cr.logical_date::timestamptz + interval '25 hour')
FROM catalog_collection_runs cr CROSS JOIN generate_series(1, %(m)s) g
ON CONFLICT DO NOTHING;
"""

INDEXES = {
    "ix_catalog_dag_runs_scope": "catalog_dag_runs (logical_date, status)",
    "ix_catalog_collection_runs_source": "catalog_collection_runs (source_id, logical_date)",
    "ix_catalog_lineage_downstream": "catalog_lineage_edges (downstream_asset_id)",
    "ix_catalog_observed_rows_asset": "catalog_observed_rows (asset_id, observed_at)",
    "ix_catalog_observed_fields_run": "catalog_observed_fields (run_id, asset_id)",
    "ix_catalog_normalized_evidence_lookup": "catalog_normalized_evidence (cluster_id, observed_at)",
    "ix_catalog_normalized_evidence_asset": "catalog_normalized_evidence (asset_id, observed_at)",
    "ix_catalog_quality_results_open": "catalog_quality_results (status, severity)",
    "ix_catalog_normalized_evidence_dup": (
        "catalog_normalized_evidence (cluster_id, source_id, resource_uid, run_id) "
        "INCLUDE (observed_at, ingested_at)"
    ),
}


def measure(conn, repeat: int) -> dict[str, float]:
    date = "2026-07-20"
    base = {
        "logical_date": date,
        "logical_ts": datetime.fromisoformat(f"{date}T03:00:00+00:00"),
        "run_id": conn.execute(text("SELECT run_id FROM catalog_collection_runs LIMIT 1")).scalar(),
        "asset_id": conn.execute(text("SELECT asset_id FROM catalog_data_assets LIMIT 1")).scalar(),
    }
    result = {}
    for name in list(CHECK_FILES) + list(LOOKUP_FILES):
        sql = load_sql(name)
        bind = {k: v for k, v in base.items() if f":{k}" in sql}
        if ":cluster_id" in sql:
            bind["cluster_id"] = "cluster-1"
        conn.execute(text(sql), bind).all()
        samples = []
        for _ in range(repeat):
            started = time.perf_counter()
            conn.execute(text(sql), bind).all()
            samples.append((time.perf_counter() - started) * 1000)
        result[name] = round(statistics.median(samples), 1)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--resources", type=int, default=500)
    parser.add_argument("--repeat", type=int, default=7)
    args = parser.parse_args()

    engine = create_engine(args.dsn, future=True)
    with engine.begin() as conn:
        runs = conn.execute(text("SELECT COUNT(*) FROM catalog_collection_runs")).scalar()
        if not runs:
            print("collection_runs 가 비어 있습니다. 먼저 catalog_run.py 로 며칠치를 적재하세요.")
            return 1
        conn.execute(text(SYNTH_ROWS % {"n": args.resources, "m": args.resources // 2}))
        conn.execute(text("ANALYZE"))
        sizes = dict(
            conn.execute(
                text("SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC")
            ).all()
        )

        for name in INDEXES:
            conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.execute(text("ANALYZE"))
        without = measure(conn, args.repeat)

        for name, spec in INDEXES.items():
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON {spec}"))
        conn.execute(text("ANALYZE"))
        with_index = measure(conn, args.repeat)

    print("\n적재 규모: " + ", ".join(f"{k} {v:,}" for k, v in list(sizes.items())[:4]))
    print(f"\n{'질의':28}{'인덱스 없음':>14}{'인덱스 있음':>14}{'배':>8}")
    print("-" * 64)
    for name in without:
        a, b = without[name], with_index[name]
        ratio = f"{a / b:.1f}x" if b else "-"
        print(f"{name:28}{a:11.1f} ms{b:11.1f} ms{ratio:>8}")
    ta, tb = sum(without.values()), sum(with_index.values())
    print("-" * 64)
    print(f"{'합계':28}{ta:11.1f} ms{tb:11.1f} ms{ta / tb:>7.1f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
