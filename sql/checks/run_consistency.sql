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
