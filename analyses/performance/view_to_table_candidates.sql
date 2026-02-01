/*
  View to Table Candidates Analysis
  
  Identifies view-materialized models that might benefit from table materialization.
  
  Good candidates:
  - Many downstream dependencies (>5 models depend on it)
  - Heavily queried by end users
  - Complex logic that gets recomputed each time
  
  Uses:
  - Elementary dbt_models for materialization and dependencies
  - Snowflake QUERY_HISTORY for user query patterns (optional)
*/

WITH view_models AS (
  SELECT 
    unique_id,
    name AS model_name,
    database_name,
    schema_name,
    materialization,
    depends_on_nodes
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
  WHERE materialization = 'view'
),

-- Parse depends_on_nodes to count downstream dependencies
all_models AS (
  SELECT 
    unique_id,
    name AS model_name,
    depends_on_nodes
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
),

-- Flatten dependencies to find downstream counts
downstream_counts AS (
  SELECT
    dep.value::STRING AS upstream_unique_id,
    COUNT(DISTINCT m.unique_id) AS downstream_count
  FROM all_models m,
       LATERAL FLATTEN(input => TRY_PARSE_JSON(m.depends_on_nodes)) dep
  WHERE dep.value::STRING LIKE 'model.%'
  GROUP BY 1
),

-- Get average execution time for views (from their runs as dependencies)
view_run_stats AS (
  SELECT
    name AS model_name,
    AVG(execution_time) AS avg_runtime_secs,
    COUNT(*) AS run_count
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_RUN_RESULTS
  WHERE generated_at > DATEADD('day', -14, CURRENT_DATE)
    AND status = 'success'
  GROUP BY 1
)

SELECT
  v.model_name,
  v.database_name || '.' || v.schema_name AS location,
  COALESCE(d.downstream_count, 0) AS downstream_models,
  ROUND(COALESCE(r.avg_runtime_secs, 0), 1) AS avg_runtime_secs,
  CASE
    WHEN COALESCE(d.downstream_count, 0) >= 5 THEN '🔥 Many dependents - consider table'
    WHEN COALESCE(d.downstream_count, 0) >= 3 AND COALESCE(r.avg_runtime_secs, 0) > 10 THEN '⚠️ Multiple deps + slow - consider table'
    WHEN COALESCE(r.avg_runtime_secs, 0) > 30 THEN '⚠️ Very slow view - consider table'
    ELSE 'ℹ️ Probably fine as view'
  END AS recommendation
FROM view_models v
LEFT JOIN downstream_counts d ON v.unique_id = d.upstream_unique_id
LEFT JOIN view_run_stats r ON v.model_name = r.model_name
WHERE COALESCE(d.downstream_count, 0) >= 1  -- Only views with at least 1 dependent
ORDER BY COALESCE(d.downstream_count, 0) DESC, r.avg_runtime_secs DESC NULLS LAST
