/*
  Incremental Candidates Analysis
  
  Identifies table-materialized models that could benefit from incremental materialization.
  
  Good candidates:
  - Slow build times (>60s average)
  - Low row growth (<10% change)
  - Has timestamp columns for incremental logic
  
  Uses:
  - Elementary model_run_results for execution times
  - Custom row_count_log for row count trends
  - Elementary dbt_columns for timestamp column detection
*/

WITH model_stats AS (
  SELECT
    name AS model_name,
    AVG(execution_time) AS avg_runtime_secs,
    MAX(execution_time) AS max_runtime_secs,
    COUNT(*) AS run_count
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_RUN_RESULTS
  WHERE generated_at > DATEADD('day', -14, CURRENT_DATE)
    AND status = 'success'
  GROUP BY 1
),

row_counts AS (
  SELECT
    model_name,
    MAX(row_count) AS latest_row_count,
    MAX(row_count) - MIN(row_count) AS row_growth_14d,
    DIV0(MAX(row_count) - MIN(row_count), NULLIF(MAX(row_count), 0)) AS growth_pct
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG
  WHERE recorded_at > DATEADD('day', -14, CURRENT_DATE)
  GROUP BY 1
),

-- Check if model has timestamp columns (good incremental candidates)
model_columns AS (
  SELECT DISTINCT
    SPLIT_PART(parent_unique_id, '.', 3) AS model_name,
    TRUE AS has_timestamp_column
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_COLUMNS
  WHERE LOWER(name) LIKE ANY ('%_date', '%_at', '%_timestamp', '%updated%', '%created%', '%loaded%')
    AND resource_type = 'model'
),

table_models AS (
  SELECT 
    name AS model_name,
    materialization
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
  WHERE materialization = 'table'
)

SELECT
  t.model_name,
  t.materialization,
  ROUND(s.avg_runtime_secs, 1) AS avg_runtime_secs,
  ROUND(s.max_runtime_secs, 1) AS max_runtime_secs,
  r.latest_row_count,
  ROUND(r.growth_pct * 100, 1) AS growth_pct,
  COALESCE(c.has_timestamp_column, FALSE) AS has_timestamp_column,
  CASE
    WHEN s.avg_runtime_secs > 60 
     AND COALESCE(r.growth_pct, 0) < 0.1 
     AND c.has_timestamp_column 
    THEN '🔥 Strong candidate'
    WHEN s.avg_runtime_secs > 30 
     AND c.has_timestamp_column 
    THEN '⚠️ Worth investigating'
    ELSE 'ℹ️ Probably fine as table'
  END AS recommendation
FROM table_models t
LEFT JOIN model_stats s ON t.model_name = s.model_name
LEFT JOIN row_counts r ON t.model_name = r.model_name
LEFT JOIN model_columns c ON t.model_name = c.model_name
WHERE s.avg_runtime_secs > 30  -- Only models taking >30s
ORDER BY s.avg_runtime_secs DESC NULLS LAST
