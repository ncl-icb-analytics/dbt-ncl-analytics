/*
  Slow Models Report
  
  Quick overview of models exceeding time thresholds.
  Useful for identifying optimization priorities.
  
  Uses:
  - Elementary model_run_results for execution times
*/

WITH run_stats AS (
  SELECT
    name AS model_name,
    materialization,
    database_name,
    schema_name,
    AVG(execution_time) AS avg_runtime_secs,
    MAX(execution_time) AS max_runtime_secs,
    MIN(execution_time) AS min_runtime_secs,
    STDDEV(execution_time) AS stddev_runtime,
    COUNT(*) AS run_count,
    SUM(execution_time) AS total_runtime_secs
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_RUN_RESULTS
  WHERE generated_at > DATEADD('day', -14, CURRENT_DATE)
    AND status = 'success'
  GROUP BY 1, 2, 3, 4
)

SELECT
  model_name,
  materialization,
  database_name || '.' || schema_name AS location,
  ROUND(avg_runtime_secs, 1) AS avg_secs,
  ROUND(max_runtime_secs, 1) AS max_secs,
  ROUND(min_runtime_secs, 1) AS min_secs,
  ROUND(stddev_runtime, 1) AS stddev_secs,
  run_count AS runs_14d,
  ROUND(total_runtime_secs / 60, 1) AS total_mins_14d,
  CASE
    WHEN avg_runtime_secs > 300 THEN '🔴 Critical (>5min)'
    WHEN avg_runtime_secs > 120 THEN '🟠 High (>2min)'
    WHEN avg_runtime_secs > 60 THEN '🟡 Medium (>1min)'
    WHEN avg_runtime_secs > 30 THEN '🟢 Low (>30s)'
    ELSE '✅ Fast'
  END AS severity,
  CASE
    WHEN materialization = 'table' AND avg_runtime_secs > 60 THEN 'Consider incremental'
    WHEN materialization = 'view' AND avg_runtime_secs > 30 THEN 'Consider table'
    WHEN stddev_runtime / NULLIF(avg_runtime_secs, 0) > 0.5 THEN 'Inconsistent - investigate'
    ELSE 'Review query optimization'
  END AS suggestion
FROM run_stats
WHERE avg_runtime_secs > 30  -- Only show models taking >30s on average
ORDER BY avg_runtime_secs DESC
