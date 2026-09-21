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
