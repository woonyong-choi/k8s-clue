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
