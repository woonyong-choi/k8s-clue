"""Daily catalog reconciliation.

Airflow 2.10 uses SQLAlchemy 1.4 internally while the catalog domain uses SQLAlchemy
2.x. The DAG therefore owns orchestration only and executes domain work in an isolated
Python interpreter. Mapped extract tasks archive one immutable snapshot per source;
downstream tasks consume that archive instead of querying each source a second time.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

LOCAL_ROOT = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("CATALOG_PROJECT_ROOT", LOCAL_ROOT))
SOURCES = ("kubernetes", "prometheus", "loki", "tempo")
DSN = os.environ.get(
    "CATALOG_DATABASE_URL", "postgresql+psycopg://postgres@postgres:5432/catalog"
)
CATALOG_PYTHON = os.environ.get("CATALOG_EXTERNAL_PYTHON", "/opt/catalog-venv/bin/python")
FIXTURE_ROOT = ROOT / "fixtures" / "catalog" / "normal"
ARCHIVE_ROOT = Path(os.environ.get("CATALOG_ARCHIVE_ROOT", ROOT / ".catalog-archive"))


@dag(
    dag_id="catalog_reconciliation_daily",
    schedule="0 3 * * *",
    start_date=datetime(2026, 7, 1, tzinfo=UTC),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["datacatalog"],
)
def catalog_reconciliation_daily():
    @task.external_python(python=CATALOG_PYTHON, expect_airflow=False)
    def extract(
        source_id: str,
        run_date: str,
        root: str,
        dsn: str,
        fixture_root: str,
        archive_root: str,
    ) -> dict[str, str]:
        import sys
        from datetime import UTC, datetime
        from pathlib import Path

        sys.path.insert(0, str(Path(root) / "src"))
        from sqlalchemy import create_engine

        from domains.datacatalog import pipeline
        from domains.datacatalog.sources import FixtureSource

        now = datetime.now(UTC)
        # 세 번째 인자는 경로가 아니라 CatalogSource 다. 어댑터로 감싸지 않으면
        # 보관 원본이 없는 날짜에서 source.fetch() 가 AttributeError 로 죽는다.
        outcome = pipeline.extract_source(
            source_id,
            run_date,
            FixtureSource(Path(fixture_root)),
            Path(archive_root),
            today=now.date().isoformat(),
        )
        with create_engine(dsn, future=True).begin() as conn:
            dag_run_id = pipeline.open_dag_run(conn, run_date, now)
            pipeline.upsert_collection_runs(conn, dag_run_id, run_date, [outcome], now)
            pipeline.archive_raw_snapshot(
                conn, dag_run_id, run_date, [outcome], Path(archive_root)
            )
        if outcome.status == "FAILED":
            raise RuntimeError(f"{source_id} extract failed")
        # XCom contains status metadata only. Raw payload remains in the archive.
        return {"source_id": source_id, "status": outcome.status}

    @task.external_python(
        python=CATALOG_PYTHON,
        expect_airflow=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def archive_and_load(
        run_date: str,
        logical_timestamp: str,
        root: str,
        dsn: str,
    ) -> str:
        import sys
        from datetime import UTC, datetime
        from pathlib import Path

        sys.path.insert(0, str(Path(root) / "src"))
        from sqlalchemy import create_engine

        from domains.datacatalog import pipeline

        now = datetime.now(UTC)
        observed_at = datetime.fromisoformat(logical_timestamp.replace("Z", "+00:00"))
        with create_engine(dsn, future=True).begin() as conn:
            dag_run_id = pipeline.open_dag_run(conn, run_date, now)
            outcomes = pipeline.load_archived_outcomes(conn, dag_run_id)
            if not any(outcome.ok for outcome in outcomes):
                raise RuntimeError("no source produced output")
            pipeline.normalize_asset_schema(conn, dag_run_id, run_date, outcomes, observed_at)
            pipeline.record_schema_observations(conn, dag_run_id, outcomes, observed_at)
            pipeline.load_catalog(conn, run_date, outcomes, now)
            pipeline.record_lineage(conn, dag_run_id, outcomes)
        return dag_run_id

    @task.external_python(
        python=CATALOG_PYTHON,
        expect_airflow=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def resolve_status(
        dag_run_id: str | None,
        run_date: str,
        root: str,
        dsn: str,
    ) -> str:
        import sys
        from datetime import UTC, datetime
        from pathlib import Path

        sys.path.insert(0, str(Path(root) / "src"))
        from sqlalchemy import create_engine

        from domains.datacatalog import pipeline

        now = datetime.now(UTC)
        with create_engine(dsn, future=True).begin() as conn:
            resolved = dag_run_id or pipeline.open_dag_run(conn, run_date, now)
            return pipeline.resolve_dag_run_status(
                conn,
                resolved,
                downstream_complete=dag_run_id is not None,
                finished_at=now,
            )

    @task.external_python(python=CATALOG_PYTHON, expect_airflow=False)
    def publish_quality_report(
        run_date: str,
        logical_timestamp: str,
        root: str,
        dsn: str,
    ) -> int:
        import sys
        from datetime import UTC, datetime
        from pathlib import Path

        sys.path.insert(0, str(Path(root) / "src"))
        from sqlalchemy import create_engine

        from domains.datacatalog import checks, pipeline

        now = datetime.now(UTC)
        observed_at = datetime.fromisoformat(logical_timestamp.replace("Z", "+00:00"))
        with create_engine(dsn, future=True).begin() as conn:
            dag_run_id = pipeline.dag_run_id_for(pipeline.DAG_ID, run_date)
            findings = checks.run_checks(conn, dag_run_id, run_date, observed_at, now)
        return len(findings)

    common = {
        "run_date": "{{ ds }}",
        "root": str(ROOT),
        "dsn": DSN,
    }
    extracted = extract.partial(
        **common,
        fixture_root=str(FIXTURE_ROOT),
        archive_root=str(ARCHIVE_ROOT),
    ).expand(source_id=list(SOURCES))
    loaded = archive_and_load(
        **common,
        logical_timestamp="{{ data_interval_start }}",
    )
    extracted >> loaded
    resolved = resolve_status(loaded, **common)
    resolved >> publish_quality_report(
        **common,
        logical_timestamp="{{ data_interval_start }}",
    )


catalog_reconciliation_daily()
