/*
  Table to View Candidates Analysis
  
  Identifies table-materialized models with NO downstream dependencies
  that could potentially be converted to views.
  
  Good candidates:
  - No downstream dbt models depend on them (leaf nodes)
  - Fast build time (<30s) - won't hurt end users
  - Rarely queried by end users
  
  Caution:
  - If heavily queried by BI tools, keep as table
  - If build time is long, keep as table (don't punish users)
  
  Uses:
  - Elementary dbt_models for dependencies
  - Elementary model_run_results for build times
  - Snowflake ACCESS_HISTORY for query frequency (optional - requires ACCOUNTADMIN)
*/

WITH table_models AS (
  SELECT 
    unique_id,
    name AS model_name,
    database_name,
    schema_name,
    materialization
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
  WHERE materialization = 'table'
),

-- Find all models that ARE dependencies of other models
upstream_models AS (
  SELECT DISTINCT
    dep.value::STRING AS unique_id
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS m,
       LATERAL FLATTEN(input => TRY_PARSE_JSON(m.depends_on_nodes)) dep
  WHERE dep.value::STRING LIKE 'model.%'
),

-- Get build time statistics
run_stats AS (
  SELECT
    name AS model_name,
    AVG(execution_time) AS avg_build_secs,
    COUNT(*) AS builds_last_14d
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_RUN_RESULTS
  WHERE generated_at > DATEADD('day', -14, CURRENT_DATE)
    AND status = 'success'
  GROUP BY 1
),

-- Get row counts for context
row_counts AS (
  SELECT
    model_name,
    MAX(row_count) AS latest_row_count
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG
  WHERE recorded_at > DATEADD('day', -7, CURRENT_DATE)
  GROUP BY 1
)

SELECT
  t.model_name,
  t.database_name || '.' || t.schema_name AS location,
  ROUND(COALESCE(r.avg_build_secs, 0), 1) AS avg_build_secs,
  COALESCE(rc.latest_row_count, 0) AS row_count,
  CASE
    WHEN u.unique_id IS NOT NULL THEN '❌ Has downstream deps - keep as table'
    WHEN COALESCE(r.avg_build_secs, 0) > 120 THEN '⚠️ Slow build - view would hurt users'
    WHEN COALESCE(r.avg_build_secs, 0) > 60 THEN 'ℹ️ Moderate build time - review manually'
    WHEN COALESCE(r.avg_build_secs, 0) < 30 
      THEN '🔥 Fast leaf node - good candidate for view'
    ELSE 'ℹ️ Review manually'
  END AS recommendation,
  CASE WHEN u.unique_id IS NULL THEN TRUE ELSE FALSE END AS is_leaf_node
FROM table_models t
LEFT JOIN upstream_models u ON t.unique_id = u.unique_id
LEFT JOIN run_stats r ON t.model_name = r.model_name
LEFT JOIN row_counts rc ON t.model_name = rc.model_name
WHERE u.unique_id IS NULL  -- Only leaf nodes (no downstream dependencies)
ORDER BY r.avg_build_secs ASC NULLS LAST
