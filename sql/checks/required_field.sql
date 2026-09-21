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
