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
