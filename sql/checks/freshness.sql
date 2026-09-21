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
