"""조회 API 가 실제로 뜨고 응답하는가.

라우터 파일이 있는 것과 앱에 붙어 도는 것은 다르다. get_connection 이
NotImplementedError 인 채로는 엔드포인트를 몇 개 세든 한 번도 실행된 적이 없다.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from domains.datacatalog.app import create_app

NOW = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)


@pytest.fixture
def client(engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM catalog_quality_results"))
        conn.execute(text("DELETE FROM catalog_data_assets"))
        conn.execute(text("DELETE FROM catalog_dag_runs"))
        conn.execute(text("DELETE FROM catalog_data_sources"))
        conn.execute(
            text(
                "INSERT INTO catalog_data_sources "
                "(source_id, name, source_type, owner, collection_interval_seconds, enabled, created_at) "
                "VALUES ('loki','Loki','log','platform',300,true,:t), "
                "       ('tempo','Tempo','trace','platform',300,true,:t)"
            ),
            {"t": NOW},
        )
        for i, day in enumerate(("2026-07-19", "2026-07-20")):
            conn.execute(
                text(
                    "INSERT INTO catalog_dag_runs "
                    "(dag_run_id, dag_id, logical_date, status, started_at, finished_at) "
                    "VALUES (:d,'catalog',:ld,'SUCCESS',:t,:t)"
                ),
                {"d": f"dag-{i}", "ld": day, "t": NOW},
            )
        # 어제 실패 1건, 오늘 실패 2건 — "미해결"이 오늘 것만 세는지 본다
        rows = [
            ("dag-0", "ops.a", "spec.replicas"),
            ("dag-1", "ops.a", "spec.replicas"),
            ("dag-1", "ops.a", "metadata.labels.app"),
        ]
        for n, (dag, asset, subject) in enumerate(rows):
            conn.execute(
                text(
                    "INSERT INTO catalog_quality_results "
                    "(result_id, dag_run_id, check_name, check_type, asset_id, subject_key, "
                    " status, severity, finding, first_seen_dag_run_id, checked_at) "
                    "VALUES (:r,:d,'03_schema_drift','SCHEMA_DRIFT',:a,:sk,"
                    "        'failed','error','TYPE_CHANGED',:d,:t)"
                ),
                {"r": f"qr-{n}", "d": dag, "a": asset, "sk": subject, "t": NOW},
            )
    with TestClient(create_app(engine=engine)) as c:
        c.headers["Authorization"] = "Bearer test-token"
        yield c


def test_앱이_뜬다(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_소스_목록이_봉투_형태로_온다(client):
    body = client.get("/v1/catalog/sources").json()
    assert {r["source_id"] for r in body["data"]} == {"loki", "tempo"}
    assert body["page"]["returned_count"] == 2
    assert body["page"]["truncated"] is False
    assert "evidence" in body


def test_커서로_다음_쪽을_받는다(client):
    first = client.get("/v1/catalog/sources", params={"limit": 1}).json()
    assert first["page"]["truncated"] is True
    cursor = first["page"]["next_cursor"]
    second = client.get("/v1/catalog/sources", params={"limit": 1, "cursor": cursor}).json()
    assert first["data"][0]["source_id"] != second["data"][0]["source_id"]


def test_한도를_넘는_요청은_거부한다(client):
    assert client.get("/v1/catalog/sources", params={"limit": 9999}).status_code == 422


def test_미해결_이슈는_최신_실행_것만_센다(client):
    """실행 전체를 대상으로 하면 어제 실패가 오늘도 미해결로 나온다."""
    body = client.get("/v1/catalog/quality/issues").json()
    assert body["page"]["total_estimated"] == 2
    assert {r["subject_key"] for r in body["data"]} == {"spec.replicas", "metadata.labels.app"}
    # 같은 자산의 두 위반이 각각 보인다. 유일 제약이 자산 단위였을 때는 1건이었다.
    assert [r["asset_id"] for r in body["data"]] == ["ops.a", "ops.a"]


def test_런_목록을_날짜로_거른다(client):
    """MCP 도구가 선언한 logical_date 를 API 가 실제로 받는다."""
    body = client.get("/v1/catalog/runs", params={"logical_date": "2026-07-20"}).json()
    assert [r["logical_date"] for r in body["data"]] == ["2026-07-20"]
    assert body["page"]["total_estimated"] == 1


def test_잘못된_날짜_형식은_거부한다(client):
    assert client.get("/v1/catalog/runs", params={"logical_date": "어제"}).status_code == 422


def test_없는_자산은_404(client):
    assert client.get("/v1/catalog/assets/없는자산").status_code == 404


def test_자산_검색이_동작한다(client, engine):
    """이 엔드포인트에 테스트가 없어서 바인드 타입 누락으로 500 이던 것을 못 잡았다."""
    from sqlalchemy import text as _t
    with engine.begin() as conn:
        conn.execute(_t("DELETE FROM catalog_data_assets"))
        conn.execute(
            _t(
                "INSERT INTO catalog_data_assets "
                "(asset_id, source_id, qualified_name, asset_type, freshness_sla_seconds, "
                "classification, owner, current_schema_version, created_at) "
                "VALUES ('a1','loki','loki.log_stream','stream',3600,'internal','platform',1,:t), "
                "       ('a2','tempo','tempo.trace','stream',3600,'internal','platform',1,:t)"
            ),
            {"t": NOW},
        )
    assert client.get("/v1/catalog/assets").status_code == 200
    body = client.get("/v1/catalog/assets", params={"q": "loki"}).json()
    assert [r["qualified_name"] for r in body["data"]] == ["loki.log_stream"]


def test_자산_검색을_원천으로_거른다(client, engine):
    from sqlalchemy import text as _t
    with engine.begin() as conn:
        conn.execute(_t("DELETE FROM catalog_data_assets"))
        conn.execute(
            _t(
                "INSERT INTO catalog_data_assets "
                "(asset_id, source_id, qualified_name, asset_type, freshness_sla_seconds, "
                "classification, owner, current_schema_version, created_at) "
                "VALUES ('a1','loki','loki.log_stream','stream',3600,'internal','platform',1,:t), "
                "       ('a2','tempo','tempo.trace','stream',3600,'internal','platform',1,:t)"
            ),
            {"t": NOW},
        )
    body = client.get("/v1/catalog/assets", params={"source": "tempo"}).json()
    assert [r["asset_id"] for r in body["data"]] == ["a2"]
    assert body["page"]["total_estimated"] == 1


def test_리소스_상태는_클러스터를_반드시_받는다(client):
    """범위를 안 좁히면 전체 이력을 정렬한다. 읽기 경로라 더 중요하다."""
    assert client.get("/v1/catalog/resources/state").status_code == 422


def test_리소스_상태가_응답한다(client):
    body = client.get("/v1/catalog/resources/state", params={"cluster_id": "cluster-1"}).json()
    assert "data" in body and "page" in body


def test_토큰_없이는_거부한다(engine):
    """MCP 가 권한을 좁힌 토큰을 보내도 받는 쪽이 안 보면 좁힌 의미가 없다."""
    with TestClient(create_app(engine=engine)) as anon:
        r = anon.get("/v1/catalog/sources")
        assert r.status_code == 401
        assert r.json()["detail"]["code"] == "missing_token"


def test_bearer_형식이_아니면_거부한다(engine):
    with TestClient(create_app(engine=engine)) as anon:
        anon.headers["Authorization"] = "test-token"
        assert anon.get("/v1/catalog/sources").status_code == 401


def test_배선이_검증기를_갈아끼울_수_있다(engine):
    """라우터가 인증 방식에 묶이면 방식이 바뀔 때마다 조회 코드를 고쳐야 한다."""
    from fastapi import HTTPException

    def only_alice(authorization: str | None = None) -> str:
        raise HTTPException(status_code=403, detail={"code": "forbidden"})

    app = create_app(engine=engine, verify_token=only_alice)
    with TestClient(app) as c:
        c.headers["Authorization"] = "Bearer bob-token"
        assert c.get("/v1/catalog/sources").status_code == 403


# 사유 코드 --------------------------------------------------------------
#
# 한때 FAILED 와 TRUNCATED 를 한 질의로 뽑아 전부 SOURCE_FAILED 로 내보냈다.
# 잘림을 실패로 합치지 않겠다는 것이 이 프로젝트의 출발점인데 응답에서 다시
# 합치고 있었다. 아래 셋은 그 회귀를 막는다.


def _degraded_run(engine, *, source_status: str) -> None:
    """PARTIAL 실행 하나와 그 아래 원천 결과 하나를 만든다."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM catalog_collection_runs"))
        conn.execute(text("UPDATE catalog_dag_runs SET status='PARTIAL' WHERE dag_run_id='dag-1'"))
        conn.execute(
            text(
                "INSERT INTO catalog_collection_runs (run_id, dag_run_id, source_id, "
                "logical_date, status, attempt, finished_at, created_at) "
                "VALUES ('cr-1','dag-1','loki','2026-07-20',:st,1,:t,:t)"
            ),
            {"st": source_status, "t": NOW},
        )


# 잘림은 실패가 아니다. 한때 둘을 한 질의로 뽑아 전부 SOURCE_FAILED 로 내보냈다.
@pytest.mark.parametrize(
    ("source_status", "expected"),
    [("TRUNCATED", "SOURCE_TRUNCATED"), ("FAILED", "SOURCE_FAILED")],
)
def test_원천_결과가_사유_코드로_그대로_옮겨진다(client, engine, source_status, expected):
    _degraded_run(engine, source_status=source_status)

    evidence = client.get("/v1/catalog/sources").json()["evidence"]

    assert evidence["reason_codes"] == [{"code": expected, "source": "loki"}]
