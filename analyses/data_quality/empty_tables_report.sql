/*
  Empty Tables Report

  Identifies table-materialized models with 0 rows.
  Empty tables may indicate:
  - Source data issues (missing upstream data)
  - Filter conditions that are too restrictive
  - Models that need to be deprecated
  - Incremental models that haven't been run

  Uses:
  - Elementary dbt_models for model metadata
  - Elementary row_count_log for row counts
  - Snowflake INFORMATION_SCHEMA for direct table stats
*/

-- Approach 1: Using Elementary row_count_log (if populated)
WITH latest_row_counts AS (
  SELECT
    model_name,
    row_count,
    recorded_at,
    ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY recorded_at DESC) AS rn
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG
),

model_info AS (
  SELECT
    name AS model_name,
    unique_id,
    database_name,
    schema_name,
    alias,
    materialization,
    path
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
  WHERE materialization IN ('table', 'incremental')
)

SELECT
  m.model_name,
  m.database_name || '.' || m.schema_name AS location,
  m.materialization,
  m.path AS file_path,
  COALESCE(r.row_count, 0) AS row_count,
  r.recorded_at AS last_checked,
  CASE
    WHEN r.row_count IS NULL THEN 'No row count data'
    WHEN r.row_count = 0 THEN 'EMPTY TABLE'
    ELSE 'Has data'
  END AS status
FROM model_info m
LEFT JOIN latest_row_counts r
  ON m.model_name = r.model_name
  AND r.rn = 1
WHERE COALESCE(r.row_count, 0) = 0
ORDER BY m.schema_name, m.model_name
