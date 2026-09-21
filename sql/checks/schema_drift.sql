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
