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
