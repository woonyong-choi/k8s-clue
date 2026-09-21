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
