/*
  Empty Tables Report (Direct Snowflake Query)

  Queries Snowflake INFORMATION_SCHEMA directly to find empty tables.
  This is useful when Elementary row counts are not available or stale.

  Note: Uses TABLE_STORAGE_METRICS for accurate row counts without
  scanning tables. Requires access to ACCOUNT_USAGE or INFORMATION_SCHEMA.

  Usage: Run directly in Snowflake (not through dbt compile)
*/

-- Get all dbt-managed tables and their row counts
WITH dbt_tables AS (
  SELECT
    name AS model_name,
    database_name,
    schema_name,
    alias,
    materialization,
    path
  FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
  WHERE materialization IN ('table', 'incremental')
),

-- Get row counts from Snowflake metadata
table_stats AS (
  SELECT
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    row_count
  FROM DATA_LAKE__NCL.INFORMATION_SCHEMA.TABLES
  WHERE table_type = 'BASE TABLE'
)

SELECT
  d.model_name,
  d.database_name || '.' || d.schema_name || '.' || COALESCE(d.alias, d.model_name) AS full_table_name,
  d.materialization,
  d.path AS file_path,
  COALESCE(t.row_count, 0) AS row_count,
  CASE
    WHEN t.table_name IS NULL THEN 'Table not found in Snowflake'
    WHEN t.row_count = 0 THEN 'EMPTY TABLE'
    ELSE 'Has data'
  END AS status,
  CASE
    WHEN t.table_name IS NULL THEN 'Model may not have been built yet'
    WHEN t.row_count = 0 AND d.materialization = 'incremental' THEN 'Check incremental logic or run --full-refresh'
    WHEN t.row_count = 0 THEN 'Check source data or filter conditions'
    ELSE NULL
  END AS recommendation
FROM dbt_tables d
LEFT JOIN table_stats t
  ON UPPER(d.database_name) = UPPER(t.database_name)
  AND UPPER(d.schema_name) = UPPER(t.schema_name)
  AND UPPER(COALESCE(d.alias, d.model_name)) = UPPER(t.table_name)
WHERE COALESCE(t.row_count, 0) = 0
ORDER BY
  CASE WHEN t.table_name IS NULL THEN 0 ELSE 1 END,  -- Missing tables first
  d.schema_name,
  d.model_name
