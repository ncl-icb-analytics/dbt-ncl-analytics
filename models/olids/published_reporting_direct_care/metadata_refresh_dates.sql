{{
    config(
        materialized='view'
    )
}}

/*
OLIDS Data Refresh Metadata

Provides centralized refresh date information for OLIDS data and dashboard tables.

Columns:
- metric_type: Type of metric (global_data_refresh, table_refresh)
- table_name: Name of dashboard table (NULL for global metrics)
- refresh_date: Date when data was last refreshed
- last_altered_timestamp: Timestamp when table was last modified (from Snowflake metadata)

The global_data_refresh represents the consensus date where at least 150 practices
have uploaded data, excluding future dates. This filters out both stale practices
and those entering incorrect future dates.

Table refresh dates are retrieved directly from Snowflake's INFORMATION_SCHEMA,
ensuring accuracy regardless of partial dbt runs.
*/

WITH global_refresh AS (
    SELECT
        'global_data_refresh' AS metric_type,
        NULL AS table_name,
        global_data_refresh_date AS refresh_date,
        NULL AS last_altered_timestamp
    FROM {{ ref('int_global_data_refresh_date') }}
),
dashboard_tables AS (
    SELECT
        'table_refresh' AS metric_type,
        table_name,
        last_altered AS last_altered_timestamp
    FROM {{ this.database }}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'OLIDS_PUBLISHED'
        AND table_type = 'BASE TABLE'
        AND table_name != 'METADATA_REFRESH_DATES'
),
table_refresh AS (
    SELECT
        metric_type,
        table_name,
        last_altered_timestamp::date AS refresh_date,
        last_altered_timestamp
    FROM dashboard_tables
)

SELECT * FROM global_refresh
UNION ALL
SELECT * FROM table_refresh
ORDER BY metric_type, table_name
