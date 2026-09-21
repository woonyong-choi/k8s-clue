[← Kyro로 돌아가기](../README.md) · [← 05](airflow-pipeline.md)

# SQL 정합성 질의

> **자료 목록 관리** · 프로젝트 종료 후 개인 작업

정합성 검사는 "이 조건에 걸리는 행이 있는가"라는 질문입니다. 검사 기준을 Python이 아니라 SQL에 둔 이유는 두 가지입니다.

**기준이 자주 바뀝니다.** 최신성 임계, 중복 판정 키, 드리프트 허용 범위는 운영하면서 계속 조정됩니다. 질의만 바뀌는 변경은 diff 가 질의 한 파일에 갇히므로, 무엇이 바뀌었는지 리뷰에서 바로 보입니다. 배포가 빨라지지는 않습니다 — 질의 파일도 같은 저장소에 있고 같이 배포됩니다.

**질의가 곧 명세입니다.** "고아 자산이란 무엇인가"를 산문으로 쓰면 코드와 어긋납니다. 질의를 읽으면 정의가 그대로 보입니다.

Python은 질의를 실행하고 결과를 `quality_results`에 적재하는 역할만 합니다.

→ [`sql/checks/`](../sql/checks/)

---

## 검사와 조회를 구분한다

파일이 10개지만 **검사는 8종입니다.** 나머지 둘(`90`·`91`)은 위반 집합을 반환하지 않는 조회 도구입니다. 초기에는 이걸 묶어서 세다가 검사 개수를 부풀린 것이라 나눴고, 이후 `04`·`08` 이 추가되어 검사가 6종에서 8종이 됐습니다.

| 파일 | 유형 | [카탈로그 검사](metadata-catalog.md#정합성-검사) |
|---|---|---|
| `source_coverage.sql` | 검사 | `SOURCE_COVERAGE` |
| `required_field.sql` | 검사 | `REQUIRED_FIELD` |
| `schema_drift.sql` | 검사 | `SCHEMA_DRIFT` |
| `unversioned_change.sql` | 검사 | `SCHEMA_DRIFT` (버전 미갱신 변경) |
| `freshness.sql` | 검사 | `FRESHNESS` |
| `lineage_break.sql` | 검사 | `LINEAGE_BREAK` |
| `run_consistency.sql` | 검사 | `RUN_CONSISTENCY` |
| `duplicate_candidates.sql` | 검사 | `RUN_CONSISTENCY` (중복 적재) |
| `latest_state.sql` | 조회 | — |
| `lineage_trace.sql` | 조회 | — |

조회 도구는 `sql/lookups/` 로 분리해 검사 실행 대상에서 뺐습니다.

---

## 검사

### 원천 커버리지

`sql/checks/source_coverage.sql`

→ [`sql/checks/source_coverage.sql`](../sql/checks/source_coverage.sql)

```sql
-- 등록된 소스와 실제 실행을 대조한다.
--
-- 7일째 아무것도 만들지 못한 소스가 결과에서 사라지면 안 된다.
-- LEFT JOIN을 쓰고 기간 조건을 ON 절에 둔다. WHERE에 두면 실행 0건인 소스가
-- 조인 후 필터에서 탈락해, 침묵하는 소스와 존재하지 않는 소스가 같은 부재로 뭉개진다.
--
-- 성공 판정에 NO_DATA를 포함한다. NO_DATA는 "정상 실행, 신규 데이터 없음"이고
-- 배치 파이프라인 문서가 FAILED와 분리한 이유가 바로 그것이다. 실패로 세면 조용한 정상 소스가
-- NEVER_SUCCEEDED(error)로 찍힌다.
--
-- TRUNCATED도 실패가 아니다. 수집 한도 문서에서 상한은 설계된 정상 동작이다.
-- 다만 상시화되면 범위 조정이 필요하므로 별도 지표로 센다.
--
-- last_run_date는 기간 밖 실행도 포함해야 한다. 기간 안에 실행이 없을 때
-- 마지막으로 언제 돌았는지가 분류에 필요한 유일한 정보다.
WITH windowed AS (
    SELECT
        s.source_id,
        s.name,
        s.enabled,
        COUNT(r.run_id) FILTER (
            WHERE r.logical_date >= (CAST(:logical_date AS date) - INTERVAL '7 days')
        )                                                                    AS runs_in_window,
        COUNT(*) FILTER (
            WHERE r.status IN ('SUCCESS','NO_DATA')
              AND r.logical_date >= (CAST(:logical_date AS date) - INTERVAL '7 days')
        )                                                                    AS healthy,
        COUNT(*) FILTER (
            WHERE r.status = 'TRUNCATED'
              AND r.logical_date >= (CAST(:logical_date AS date) - INTERVAL '7 days')
        )                                                                    AS truncated,
        COUNT(*) FILTER (
            WHERE r.status = 'FAILED'
              AND r.logical_date >= (CAST(:logical_date AS date) - INTERVAL '7 days')
        )                                                                    AS failed,
        MAX(r.logical_date)                                                  AS last_run_date
    FROM catalog_data_sources AS s
    LEFT JOIN catalog_collection_runs AS r ON r.source_id = s.source_id
    GROUP BY s.source_id, s.name, s.enabled
)
SELECT
    source_id, name, runs_in_window, healthy, truncated, failed, last_run_date,
    CASE
        WHEN enabled AND runs_in_window = 0                      THEN 'ENABLED_BUT_SILENT'
        WHEN NOT enabled AND runs_in_window > 0                  THEN 'DISABLED_BUT_RUNNING'
        WHEN runs_in_window > 0 AND healthy = 0                  THEN 'NEVER_HEALTHY'
        -- 상시 잘림을 DEGRADED 보다 먼저 본다. TRUNCATED 는 healthy 에 안 들어가므로
        -- 잘림이 절반을 넘으면 healthy 비율은 반드시 0.8 아래다. 순서를 반대로 두면
        -- DEGRADED 가 항상 먼저 잡아서 CHRONICALLY_TRUNCATED 는 어떤 입력에서도
        -- 나오지 않는다. 둘을 나눈 이유가 "잘림은 실패가 아니라 범위 조정 신호" 인데
        -- 순서 하나로 그 구분이 사라진다.
        WHEN runs_in_window > 0
             AND truncated::numeric / runs_in_window > 0.5       THEN 'CHRONICALLY_TRUNCATED'
        WHEN runs_in_window > 0
             AND healthy::numeric / runs_in_window < 0.8         THEN 'DEGRADED'
    END AS finding
FROM windowed
WHERE (enabled AND runs_in_window = 0)
   OR (NOT enabled AND runs_in_window > 0)
   OR (runs_in_window > 0 AND healthy::numeric / runs_in_window < 0.8)
   OR (runs_in_window > 0 AND truncated::numeric / runs_in_window > 0.5);
```

`ENABLED_BUT_SILENT`이 이 질의의 존재 이유입니다. 성공률이 떨어지는 것보다 **아무 소리도 안 나는 쪽이 더 위험합니다.**

미등록 원천을 잡는 분기는 넣지 않았습니다. `collection_runs.source_id`에 외래키가 있어 **스키마가 그 상태를 허용하지 않습니다.** 검사할 수 없는 상태를 검사한다고 적으면 안 됩니다. 외래키를 푸는 편이 나은지는 아직 판단하지 않았습니다.

### 필수 필드 누락

`sql/checks/required_field.sql`

→ [`sql/checks/required_field.sql`](../sql/checks/required_field.sql)

```sql
-- 자산 계약에 required로 등록된 필드가 실제 행에 없는 경우를 찾는다.
--
-- 실행 범위를 고정한다. 고정하지 않으면 전체 이력을 스캔하고,
-- 자산 하나가 잘못되면 quality_results에 수백만 행을 밀어 넣는다.
--
-- 행 단위로 반환하지 않고 (자산, 필드) 단위로 집계한다.
-- 원인 파악에 필요한 것은 위반 건수와 표본이지 전체 목록이 아니다.
WITH required_fields AS (
    SELECT f.asset_id, f.field_path
    FROM catalog_asset_fields AS f
    JOIN catalog_data_assets  AS a
      ON a.asset_id = f.asset_id
     AND a.current_schema_version = f.schema_version
    WHERE f.required
),
violations AS (
    SELECT o.asset_id, rf.field_path, o.row_id
    FROM catalog_observed_rows   AS o
    JOIN required_fields AS rf ON rf.asset_id = o.asset_id
    WHERE o.run_id = :run_id
      AND NOT EXISTS (
          SELECT 1
          FROM catalog_observed_fields AS f
          WHERE f.row_id     = o.row_id
            AND f.field_path = rf.field_path
      )
)
SELECT
    asset_id,
    field_path,
    COUNT(*)                                        AS violation_count,
    (ARRAY_AGG(row_id ORDER BY row_id))[1:5]        AS sample_row_ids
FROM violations
GROUP BY asset_id, field_path
ORDER BY violation_count DESC;
```

### 스키마 드리프트

`sql/checks/schema_drift.sql`

→ [`sql/checks/schema_drift.sql`](../sql/checks/schema_drift.sql)

```sql
-- 등록 계약과 실제 관측 필드를 양방향으로 대조한다.
--
-- declared를 이번 실행이 실제로 관측한 자산으로 한정한다.
-- 한정하지 않으면, 소스 하나가 실패한 PARTIAL 실행에서
-- 관측되지 않은 모든 자산의 모든 필드가 MISSING_FIELD(error)로 쏟아진다.
-- 부분 실패를 보존하겠다는 설계가 부분 실패마다 오탐 폭풍을 내는 셈이 된다.
--
-- 타입 비교에 IS DISTINCT FROM을 쓴다. <> 는 NULL 앞에서 NULL을 반환해,
-- 타입을 판별하지 못하게 된 필드(가장 유력한 드리프트 신호)를 통과시킨다.
WITH covered AS (
    SELECT DISTINCT asset_id
    FROM catalog_observed_fields
    WHERE run_id = :run_id
),
declared AS (
    SELECT f.asset_id, f.field_path, f.data_type
    FROM catalog_asset_fields AS f
    JOIN catalog_data_assets  AS a
      ON a.asset_id = f.asset_id
     AND a.current_schema_version = f.schema_version
    JOIN covered      AS c ON c.asset_id = f.asset_id
),
observed AS (
    SELECT DISTINCT asset_id, field_path, data_type
    FROM catalog_observed_fields
    WHERE run_id = :run_id
)
SELECT
    COALESCE(d.asset_id,   o.asset_id)   AS asset_id,
    COALESCE(d.field_path, o.field_path) AS field_path,
    d.data_type AS declared_type,
    o.data_type AS observed_type,
    CASE
        WHEN o.field_path IS NULL THEN 'MISSING_FIELD'
        WHEN d.field_path IS NULL THEN 'UNDECLARED_FIELD'
        ELSE 'TYPE_CHANGED'
    END AS drift_type
FROM declared AS d
FULL OUTER JOIN observed AS o
  ON  o.asset_id   = d.asset_id
  AND o.field_path = d.field_path
WHERE o.field_path IS NULL
   OR d.field_path IS NULL
   OR d.data_type IS DISTINCT FROM o.data_type;
```

### 버전을 올리지 않은 변경

`sql/checks/unversioned_change.sql`

→ [`sql/checks/unversioned_change.sql`](../sql/checks/unversioned_change.sql)

```sql
-- 스키마가 바뀌면 버전을 올리는 것이 규칙이다. 규칙을 지키지 않은 변경이 진짜 문제다.
--
-- asset_fields를 그대로 GROUP BY 하면 이 검사는 절대 작동하지 않는다.
-- 적재가 ON CONFLICT DO UPDATE라서 이전 세대 해시가 제자리에서 덮이기 때문이다.
-- 그래서 schema_observations를 append-only로 따로 둔다.
-- 계약 이력을 남기지 않으면 "바뀌었다"를 판정할 기준점 자체가 없다.
SELECT
    o.asset_id,
    o.schema_version,
    COUNT(DISTINCT o.schema_hash)                       AS hash_variants,
    MIN(o.first_seen_at)                                AS first_seen_at,
    MAX(o.first_seen_at)                                AS last_changed_at,
    ARRAY_AGG(DISTINCT o.schema_hash)                   AS hashes,
    ARRAY_AGG(DISTINCT o.first_seen_run_id)             AS from_runs
FROM catalog_schema_observations AS o
GROUP BY o.asset_id, o.schema_version
HAVING COUNT(DISTINCT o.schema_hash) > 1;
```

`schema_observations`는 `(asset_id, schema_version, schema_hash)`가 유일 키인 append-only 테이블입니다. 같은 계약이 반복 관측되면 행이 늘지 않고, **계약이 바뀌면 행이 하나 생깁니다.**

### 최신성 위반

`sql/checks/freshness.sql`

→ [`sql/checks/freshness.sql`](../sql/checks/freshness.sql)

```sql
-- 자산 단위로 마지막 관측 시각을 본다.
--
-- collection_runs를 source_id로 조인하면 안 된다. 같은 소스의 건강한 자산이
-- 죽은 자산을 가린다. 매일 성공하는 소스에 속한 자산은 영원히 신선해 보인다.
--
-- 기준 시각은 배치 완료 시각이 아니라 데이터의 관측 시각이다.
-- finished_at을 쓰면 일 배치에서 staleness가 하루를 넘지 못해 검사가 발화하지 않는다.
--
-- 관측 집합도 logical_ts로 잘라야 한다. 자르지 않으면 backfill 시
-- 처리 날짜보다 나중에 들어온 행이 과거를 건강하게 보이게 만든다.
-- 같은 구간을 다른 날 재실행했을 때 결과가 달라지면 backfill이 무의미해진다.
--
-- 관측 원천이 둘이다. 원천 자산은 observed_rows에, 정규화·파생 자산은
-- normalized_evidence에 들어간다. 한쪽만 보면 정규화 자산이 영구히 NEVER_OBSERVED가 된다.
WITH last_seen AS (
    SELECT asset_id, MAX(observed_at) AS last_observed_at
    FROM (
        SELECT asset_id, observed_at FROM catalog_observed_rows
        WHERE observed_at <= :logical_ts
        UNION ALL
        SELECT asset_id, observed_at FROM catalog_normalized_evidence
        WHERE observed_at <= :logical_ts
    ) AS all_observations
    GROUP BY asset_id
)
SELECT
    a.asset_id,
    a.qualified_name,
    a.freshness_sla_seconds,
    l.last_observed_at,
    EXTRACT(EPOCH FROM (:logical_ts - l.last_observed_at))::bigint AS staleness_seconds,
    CASE WHEN l.last_observed_at IS NULL THEN 'NEVER_OBSERVED' ELSE 'STALE' END AS finding
FROM catalog_data_assets AS a
LEFT JOIN last_seen AS l ON l.asset_id = a.asset_id
WHERE l.last_observed_at IS NULL
   OR EXTRACT(EPOCH FROM (:logical_ts - l.last_observed_at)) > a.freshness_sla_seconds;
```

`NOW()`가 아니라 `:logical_ts`를 씁니다. 그리고 관측 집합도 같은 기준으로 자릅니다. **오른쪽 항만 고치고 데이터셋을 열어 두면 미래 데이터가 과거를 가립니다.**

### 리니지 단절

`sql/checks/lineage_break.sql`

→ [`sql/checks/lineage_break.sql`](../sql/checks/lineage_break.sql)

```sql
-- 세 가지를 함께 잡는다.
--   1) 정규화 자산인데 upstream 간선이 없다
--   2) 존재하지 않는 자산을 가리키는 간선이 있다
--   3) 간선은 있는데 확인된 지 오래됐다
--
-- 3번을 위해 lineage_edges.run_id를 collection_runs에 조인한다.
-- run_id를 저장만 하고 조인하지 않으면 "언제 확인된 관계인가"에 답할 수 없다.
SELECT a.asset_id, a.qualified_name, 'NO_UPSTREAM' AS finding, NULL::text AS detail
FROM catalog_data_assets AS a
WHERE a.asset_type IN ('normalized', 'derived')
  AND NOT EXISTS (
      SELECT 1 FROM catalog_lineage_edges e WHERE e.downstream_asset_id = a.asset_id
  )

UNION ALL

SELECT e.downstream_asset_id, NULL, 'DANGLING_EDGE', e.upstream_asset_id
FROM catalog_lineage_edges AS e
WHERE NOT EXISTS (
    SELECT 1 FROM catalog_data_assets a WHERE a.asset_id = e.upstream_asset_id
)

UNION ALL

SELECT e.downstream_asset_id, NULL, 'STALE_EDGE',
       'confirmed_at=' || r.finished_at::text
FROM catalog_lineage_edges  AS e
JOIN catalog_collection_runs AS r ON r.run_id = e.run_id
WHERE r.finished_at < (:logical_ts - INTERVAL '7 days');
```

### 실행 정합성

`sql/checks/run_consistency.sql`

→ [`sql/checks/run_consistency.sql`](../sql/checks/run_consistency.sql)

```sql
-- 실행이 실제보다 좋게 기록된 경우를 찾는다.
--
-- 첫 번째 조건이 이 검사의 존재 이유다. 소스 하나가 실패했는데 DAG 실행이
-- SUCCESS로 남으면, 부분 실패를 보존한다는 설계 전체가 무효가 된다.
--
-- 두 번째 조건은 아무것도 검사하지 않은 실행이다. "검사하지 않은 것과
-- 검사해서 통과한 것은 다르다"고 했으면, 검사 결과가 0건인 성공 실행도 잡아야 한다.
--
-- 실패한 검사가 있다고 실행을 무조건 위반으로 만들지는 않는다. warning까지 승격하면
-- 영구 warning 하나가 이후 모든 실행을 영구히 붉게 만들고, 그러면 아무도 안 본다.
-- error 심각도이면서 이번 실행에서 처음 발생한 것만 본다.
SELECT
    d.dag_run_id,
    d.logical_date,
    d.status,
    CASE
        WHEN EXISTS (SELECT 1 FROM catalog_collection_runs c
                     WHERE c.dag_run_id = d.dag_run_id
                       AND c.status IN ('FAILED','TRUNCATED'))
             AND d.status = 'SUCCESS'
            THEN 'SOURCE_FAILED_BUT_RUN_SUCCESS'
        WHEN d.status = 'SUCCESS' AND NOT EXISTS (
                SELECT 1 FROM catalog_quality_results q WHERE q.dag_run_id = d.dag_run_id)
            THEN 'SUCCESS_WITHOUT_ANY_CHECK'
        WHEN d.finished_at IS NULL
            THEN 'TERMINAL_WITHOUT_FINISH'
        WHEN d.status = 'SUCCESS' AND NOT EXISTS (
                SELECT 1 FROM catalog_raw_snapshots s
                JOIN catalog_collection_runs c ON c.run_id = s.run_id
                WHERE c.dag_run_id = d.dag_run_id)
            THEN 'SUCCESS_WITHOUT_SNAPSHOT'
        ELSE 'SUCCESS_WITH_NEW_ERRORS'
    END AS finding
FROM catalog_dag_runs AS d
WHERE d.logical_date = :logical_date
  AND d.status IN ('SUCCESS','PARTIAL')
  AND (
        (d.status = 'SUCCESS' AND EXISTS (
            SELECT 1 FROM catalog_collection_runs c
            WHERE c.dag_run_id = d.dag_run_id
              AND c.status IN ('FAILED','TRUNCATED')))
     OR (d.status = 'SUCCESS' AND NOT EXISTS (
            SELECT 1 FROM catalog_quality_results q WHERE q.dag_run_id = d.dag_run_id))
     OR d.finished_at IS NULL
     OR (d.status = 'SUCCESS' AND NOT EXISTS (
            SELECT 1 FROM catalog_raw_snapshots s
            JOIN catalog_collection_runs c ON c.run_id = s.run_id
            WHERE c.dag_run_id = d.dag_run_id))
     OR (d.status = 'SUCCESS' AND EXISTS (
            SELECT 1 FROM catalog_quality_results q
            WHERE q.dag_run_id = d.dag_run_id
              AND q.status   = 'failed'
              AND q.severity = 'error'
              AND q.first_seen_dag_run_id = d.dag_run_id))
  );
```

`first_seen_dag_run_id`가 필요한 이유가 있습니다. 이 컬럼이 없으면 **한 번 발생한 영구 위반이 이후 모든 실행을 정합성 위반으로 만듭니다.** 검사들이 서로를 오염시켜 일주일이면 전부 붉어지고, 그 뒤로는 신호가 아니라 잡음이 됩니다.

### 중복 적재 후보

`sql/checks/duplicate_candidates.sql`

→ [`sql/checks/duplicate_candidates.sql`](../sql/checks/duplicate_candidates.sql)

```sql
-- 같은 대상이 서로 다른 실행에서 다시 적재된 경우를 찾는다.
--
-- 처음에는 (cluster_id, source_id, resource_uid, observed_at) 로 묶고
-- COUNT(*) > 1 을 봤다. 그런데 그 네 컬럼은 uq_catalog_normalized_evidence 의
-- 유일 키다. 제약이 이미 막고 있는 것을 검사가 다시 세고 있었던 것이라,
-- 이 질의는 어떤 데이터에서도 0행이었다. 잡는 것 없이 시간만 썼다.
--
-- 유일 제약이 막지 못하는 중복은 observed_at 이 미세하게 다른 재관측이다.
-- 재시도나 backfill 이 원천 타임스탬프를 다시 계산하면 정확히 그 모양이 된다.
-- 그래서 시각을 날짜 단위로 뭉개고, 같은 날짜를 서로 다른 run 이 적재한 것만 남긴다.
-- 날짜로 뭉개지 않고 그냥 시각만 빼면, 매일 관측되는 정상 리소스가 전부 걸린다.
-- 오래 살아 있는 리소스는 실행 수만큼 행이 있는 것이 정상이기 때문이다.
--
-- 두 단계로 나눈 이유:
--
-- 기간 조건을 GROUP BY 앞의 WHERE 에 두면 안 된다. 원본은 5일 전이고 복제본만
-- 어제 들어온 쌍에서 원본이 먼저 걸러져 "1건짜리 그룹" 으로 보인다. 재시도와
-- backfill 이 만드는 중복이 정확히 그 모양이라 잡아야 할 것을 놓친다.
--
-- 그렇다고 HAVING 에만 두면 매 실행이 테이블 전량을 읽는다. 관측 470만 행에서
-- 결과가 0행인데 470,421행을 전부 읽고 전부 버렸다.
--
-- 그래서 최근 적재분에서 후보 키만 먼저 뽑고(recent), 그 키에 대해서만 전체
-- 기간을 조회한다. 원본이 5일 전이어도 복제본이 최근에 들어왔으면 2단계에서
-- 같은 그룹으로 묶이므로 놓치는 사례가 없다. 측정은 docs/load-and-design-limits.md 에 있다.
WITH recent AS (
    SELECT DISTINCT
        cluster_id,
        source_id,
        resource_uid,
        DATE_TRUNC('day', observed_at) AS observed_day
    FROM catalog_normalized_evidence
    WHERE ingested_at >= (:logical_ts - INTERVAL '2 days')
)
SELECT
    e.cluster_id,
    e.source_id,
    e.resource_uid,
    DATE_TRUNC('day', e.observed_at) AS observed_day,
    COUNT(*)                     AS observation_count,
    COUNT(DISTINCT e.run_id)     AS run_count,
    MIN(e.observed_at)           AS first_observed_at,
    MAX(e.observed_at)           AS last_observed_at,
    MAX(e.ingested_at)           AS last_ingested_at,
    ARRAY_AGG(DISTINCT e.run_id) AS from_runs
FROM catalog_normalized_evidence AS e
JOIN recent AS r
  ON  r.cluster_id   = e.cluster_id
  AND r.source_id    = e.source_id
  AND r.resource_uid = e.resource_uid
  AND r.observed_day = DATE_TRUNC('day', e.observed_at)
GROUP BY e.cluster_id, e.source_id, e.resource_uid, DATE_TRUNC('day', e.observed_at)
HAVING COUNT(DISTINCT e.run_id) > 1
ORDER BY observation_count DESC;
```

`normalized_evidence`에 유일 제약이 있어 정상 경로에서는 중복이 생기지 않습니다. 이 검사는 **제약이 빠진 채 배포됐거나 수동 적재가 있었던 경우를 잡는 안전망**입니다.

---

## 조회 도구

검사가 아닙니다. `quality_results`에 적재하지 않습니다.

### 리소스별 최신 상태

`sql/lookups/latest_state.sql`

→ [`sql/lookups/latest_state.sql`](../sql/lookups/latest_state.sql)

```sql
-- 리소스별로 가장 최근 관측 행 하나를 고른다.
--
-- SUCCESS 필터를 CTE 안에 두면 안 된다. 그러면 1분 전 PARTIAL 행을 버리고
-- 2시간 전 SUCCESS 행을 "현재 상태"로 반환하면서, 더 새 관측이 있었다는 사실을
-- 알리지 않는다. 전 행을 대상으로 순위를 매기고 상태를 함께 노출한다.
--
-- 정렬에 tiebreaker가 필요하다. observed_at 동률은 실제로 발생하며
-- (중복 적재 검사가 바로 그 경우를 찾는다) 없으면 같은 입력에 다른 답이 나온다.
--
-- 상태 비교에 IS DISTINCT FROM을 쓴다. <> 는 status가 NULL일 때 NULL을 반환하고
-- 소비자는 그걸 false로 읽는다. 가장 불완전한 상태가 완전으로 보고된다.
--
-- 조회 범위를 반드시 좁힌다. 리소스 상태를 읽는 경로를 염두에 두고 만들었다.
-- WHERE 없이 전체 이력을 정렬하면 데이터가 쌓일수록 응답이 선형으로 느려진다.
--
-- GET /v1/catalog/resources/state 와 MCP get_resource_state 가 이 질의를 쓴다.
-- 읽기 경로에 있으므로 범위를 좁히는 것이 특히 중요하다.
-- 적재 시점에 상태를 유지하는 테이블로 옮기면 더 빨라진다 —
-- 측정과 설계는 docs/load-and-design-limits.md 에 있다.
WITH ranked AS (
    SELECT
        e.evidence_id,
        e.cluster_id,
        e.source_id,
        e.resource_uid,
        e.observed_at,
        e.collection_status,
        ROW_NUMBER() OVER (
            PARTITION BY e.cluster_id, e.source_id, e.resource_uid
            ORDER BY e.observed_at DESC, e.evidence_id DESC
        ) AS rn
    FROM catalog_normalized_evidence AS e
    WHERE e.cluster_id  = :cluster_id
      AND e.observed_at >= (:logical_ts - INTERVAL '7 days')
)
SELECT
    evidence_id, cluster_id, source_id, resource_uid, observed_at, collection_status,
    (collection_status IS DISTINCT FROM 'SUCCESS') AS latest_is_incomplete
FROM ranked
WHERE rn = 1;
```

`latest_is_incomplete`가 이 질의의 핵심입니다. **최신 행이 불완전하다는 사실을 감추지 않고 함께 반환합니다.**

### 리니지 역추적

`sql/lookups/lineage_trace.sql`

→ [`sql/lookups/lineage_trace.sql`](../sql/lookups/lineage_trace.sql)

```sql
-- 정규화 자산에서 원본 자산까지 경로를 거슬러 올라간다.
--
-- 순환 참조가 있으면 무한 루프가 되므로 방문 경로를 누적해 차단한다.
-- 다이아몬드 형태에서 같은 조상이 여러 경로로 중복 반환되므로 DISTINCT ON으로 하나만 남긴다.
--
-- DISTINCT ON에는 완전한 tiebreaker가 필요하다. 같은 쌍에 간선이 여럿이면
-- (재확인될 때마다 run_id별로 한 행이 생긴다) depth가 동률이고,
-- 그러면 어느 run_id가 뽑히는지가 물리적 행 순서에 달린다.
-- 하필 그때 뒤집히는 값이 edge_stale이라, 리니지 신선도가 heap 순서로 결정된다.
-- 가장 최근에 확인된 간선을 고른다.
--
-- collection_runs를 LEFT JOIN한다. INNER JOIN이면 run_id가 실행 이력에 없는 간선이
-- 결과에서 사라진다. 출처를 모르는 간선은 오래된 간선보다 나쁘지, 숨겨야 할 것이 아니다.
WITH RECURSIVE upstream AS (
    SELECT
        l.downstream_asset_id AS origin,
        l.upstream_asset_id   AS ancestor,
        l.transformation,
        l.run_id,
        1 AS depth,
        ARRAY[l.downstream_asset_id, l.upstream_asset_id] AS path
    FROM catalog_lineage_edges AS l
    WHERE l.downstream_asset_id = :asset_id

    UNION ALL

    SELECT
        u.origin, l.upstream_asset_id, l.transformation, l.run_id,
        u.depth + 1, u.path || l.upstream_asset_id
    FROM catalog_lineage_edges AS l
    JOIN upstream      AS u ON u.ancestor = l.downstream_asset_id
    WHERE NOT l.upstream_asset_id = ANY(u.path)
      AND u.depth < 10
)
SELECT DISTINCT ON (u.origin, u.ancestor)
    u.origin,
    u.ancestor,
    u.transformation,
    u.depth,
    u.run_id,
    c.finished_at AS edge_confirmed_at,
    (c.finished_at IS NULL
     OR c.finished_at < (:logical_ts - INTERVAL '7 days')) AS edge_stale,
    (a.asset_id IS NULL)                                   AS ancestor_missing
FROM upstream             AS u
LEFT JOIN catalog_collection_runs AS c ON c.run_id   = u.run_id
LEFT JOIN catalog_data_assets     AS a ON a.asset_id = u.ancestor
ORDER BY u.origin, u.ancestor, u.depth, c.finished_at DESC NULLS LAST, u.run_id DESC;
```

`ancestor_missing`을 함께 반환합니다. 리니지 단절 검사가 dangling 간선을 잡지만, **사람이 실제로 리니지를 볼 때 쓰는 것은 이 질의**입니다. 검사 결과를 따로 찾아보게 만들지 않습니다.

---

## 측정

관측 데이터를 쌓아 두고 인덱스 유무로 나눠 쟀습니다. **합성 데이터입니다** — 분포와 카디널리티가 실제와 다르므로 [측정과 한계](load-and-design-limits.md)의 고지를 함께 읽어야 합니다. `make catalog-bench` 로 재현합니다.

적재 규모는 `observed_fields` 603,618행 · `observed_rows` 120,762행 · `normalized_evidence` 60,762행입니다. 각 질의를 7회 실행한 중앙값입니다.

| 질의 | 인덱스 없음 | 인덱스 있음 | 배 |
|---|---:|---:|---:|
| 01 원천 커버리지 | 1.2 ms | 0.9 ms | 1.3x |
| 02 필수 필드 누락 | 4.9 ms | 4.0 ms | 1.2x |
| **03 스키마 드리프트** | **110.9 ms** | **2.5 ms** | **44.4x** |
| 04 버전 미갱신 변경 | 0.6 ms | 0.6 ms | 1.0x |
| 05 최신성 위반 | 41.5 ms | 29.7 ms | 1.4x |
| 06 리니지 단절 | 1.0 ms | 0.6 ms | 1.7x |
| 07 실행 정합성 | 1.7 ms | 1.0 ms | 1.7x |
| 08 중복 적재 후보 | 235.9 ms | 95.7 ms | 2.5x |
| 90 리소스별 최신 상태 | 25.9 ms | 14.2 ms | 1.8x |
| 91 리니지 역추적 | 0.9 ms | 0.8 ms | 1.1x |
| **합계** | **424.5 ms** | **150.0 ms** | **2.8x** |

스키마 드리프트이 44배인 이유는 `observed_fields` 60만 행을 `run_id`·`asset_id` 로 좁히는 질의라서입니다. 인덱스가 없으면 매번 전체를 읽습니다.

중복 적재 후보 검사는 인덱스로 2.5배가 되지만 여전히 가장 느립니다. 그룹 키가 세 컬럼이라 정렬을 완전히 피할 수 없습니다. 커버링 인덱스에 `INCLUDE (observed_at, ingested_at)` 을 넣어 힙 접근을 없앤 것이 이 폭의 대부분입니다.

**중복 적재 후보을 고치기 전에는 이 질의가 전체의 52% 를 쓰면서 아무것도 잡지 못했습니다.** 사유는 위 [중복 적재 후보 항목](#중복-적재-후보)에 있습니다.

→ 벤치마크 코드: [`scripts/catalog_bench.py`](../scripts/catalog_bench.py)

---

## 실행

```bash
make catalog-sql
```

검사 8종은 결과를 `quality_results`에 적재합니다. **위반 0건이어도 통과 결과를 남깁니다.** 검사하지 않은 것과 검사해서 통과한 것을 구분하기 위해서입니다. [수집 완전성 계약](collection-contract.md)의 원칙과 같습니다.

## 검증

`make catalog-verify`가 질의마다 **음성 케이스와 양성 케이스를 모두** 돌립니다.

| 검사 | 양성 fixture | 음성 fixture |
|---|---|---|
| 01 | 7일 무실행 활성 원천, 비활성인데 도는 원천, 성공률 저하 | 정상 원천, **전부 `NO_DATA`인 원천**, **`TRUNCATED` 섞인 원천** |
| 02 | required 필드 누락 행 | 완전한 행, 구버전 계약만 어긋난 행 |
| 03 | 타입 변경, 필드 추가, 필드 삭제, 타입 NULL | 일치하는 계약, **이번 실행에서 미관측인 자산** |
| 04 | 같은 버전에 다른 해시 2건 | 버전과 함께 바뀐 해시, 동일 계약 반복 관측 |
| 05 | SLA 초과 자산, 미관측 자산, **정규화 자산** | 신선한 자산, **처리일 이후 유입 행만 있는 자산** |
| 06 | upstream 없는 정규화 자산, dangling 간선, 오래된 간선, **run_id 미상 간선** | 정상 리니지 |
| 07 | 원천 실패인데 SUCCESS, 검사 0건 SUCCESS, 스냅샷 없는 SUCCESS | 일관된 실행, **기존 error가 남아 있는 정상 실행** |
| 08 | 같은 창 안 중복, **원본이 5일 전인 중복** | 유일 키 |
| 90 | PARTIAL 최신, **status NULL 최신**, observed_at 동률 | SUCCESS 최신 |
| 91 | 순환, 다이아몬드, **같은 쌍에 간선 2개**, 없는 조상 | 단일 경로 |

**굵게 표시한 것이 실제로 결함을 잡아낸 fixture 입니다.** 처음에는 없었고, 각 질의가 자기 주석이 주장하는 것을 실제로 검출하는지 확인하는 과정에서 추가했습니다.

특히 음성 fixture가 중요합니다. **양성만 돌리면 "항상 참을 반환하는 질의"도 통과합니다.** 원천 커버리지가 `NO_DATA` 원천을 실패로 세던 것, 최신성 위반이 backfill에서 미래 데이터를 보던 것, 리소스별 최신 상태 질의가 NULL 상태를 완전으로 보고하던 것은 전부 음성 fixture에서 잡혔습니다.

## 이 작업이 증명하는 것

- **JOIN · GROUP BY · CTE · 윈도 함수 · 재귀 CTE**를 데이터 품질 검증에 사용
- `NULL` 비교, 조인 방향, 필터 위치가 결과를 뒤집는 경우에 대한 이해
- 배치 재처리 시 **시점 기준을 고정**해야 하는 이유
- 검사 자체가 옳은지 확인하기 위한 **음성 fixture 설계**

## 남은 것

- 인덱스는 이후 추가했습니다(`models.py` 에 9개). 위 측정표가 그 전후 비교입니다. **중복 적재 후보 검사는 인덱스로 풀리지 않습니다** — 지금은 최근 2일 적재분에서 후보를 뽑는 2단계 질의를 씁니다. 빨라진 대신 그 창 밖에서 원본과 복제본이 모두 적재된 중복은 놓칩니다. [측정과 한계](load-and-design-limits.md) 참고
- `normalized_evidence`·`observed_rows`에 파티셔닝이 없습니다. 리소스별 최신 상태 질의는 조회 범위를 좁혔지만 근본 해법은 시간 파티셔닝입니다
- 임계값(성공률 0.8, 잘림 비율 0.5, 재귀 깊이 10, 간선 신선도 7일)이 질의에 상수로 박혀 있습니다. 자산별로 다르게 두려면 설정 테이블이 필요합니다
- 스키마 드리프트 검사는 `:run_id` 범위 안에서만 봅니다. 여러 실행에 걸친 추세는 별도 집계가 필요합니다
- 버전을 올리지 않은 변경 검사에는 해소 경로가 없습니다. append-only 테이블이라 6월의 미등록 변경 한 건이 오늘도 계속 발화합니다. 확인 처리 테이블이 필요합니다

---

[다음: 카탈로그 API와 MCP →](catalog-api-mcp.md)
