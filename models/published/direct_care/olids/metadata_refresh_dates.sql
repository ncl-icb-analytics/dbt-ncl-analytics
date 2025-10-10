{{
    config(
        materialized='view'
    )
}}

/*
OLIDS Data Refresh Metadata

Provides centralised refresh date information for OLIDS data and dashboard tables.

Columns:
- metric_type: Type of metric (global_data_refresh, table_refresh)
- database_name: Snowflake database name (NULL for global metrics)
- schema_name: Snowflake schema name (NULL for global metrics)
- table_name: Table name (NULL for global metrics)
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
        NULL AS database_name,
        NULL AS schema_name,
        NULL AS table_name,
        global_data_refresh_date AS refresh_date,
        NULL AS last_altered_timestamp,
        0 AS sort_order
    FROM {{ ref('int_global_data_refresh_date') }}
),
published_reporting_tables AS (
    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered AS last_altered_timestamp,
        1 AS sort_order
    FROM {{ this.database }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name != 'METADATA_REFRESH_DATES'
),
reporting_tables AS (
    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered AS last_altered_timestamp,
        2 AS sort_order
    FROM {{ this.database.replace('PUBLISHED_REPORTING__DIRECT_CARE', 'REPORTING') }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema LIKE 'OLIDS_%'
),
modelling_tables AS (
    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered AS last_altered_timestamp,
        3 AS sort_order
    FROM {{ this.database.replace('PUBLISHED_REPORTING__DIRECT_CARE', 'MODELLING') }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema LIKE 'OLIDS_%'
),
table_refresh AS (
    SELECT
        metric_type,
        database_name,
        schema_name,
        table_name,
        last_altered_timestamp::date AS refresh_date,
        last_altered_timestamp,
        sort_order
    FROM published_reporting_tables

    UNION ALL

    SELECT
        metric_type,
        database_name,
        schema_name,
        table_name,
        last_altered_timestamp::date AS refresh_date,
        last_altered_timestamp,
        sort_order
    FROM reporting_tables

    UNION ALL

    SELECT
        metric_type,
        database_name,
        schema_name,
        table_name,
        last_altered_timestamp::date AS refresh_date,
        last_altered_timestamp,
        sort_order
    FROM modelling_tables
)

SELECT
    metric_type,
    database_name,
    schema_name,
    table_name,
    refresh_date,
    last_altered_timestamp,
    sort_order
FROM global_refresh

UNION ALL

SELECT
    metric_type,
    database_name,
    schema_name,
    table_name,
    refresh_date,
    last_altered_timestamp,
    sort_order
FROM table_refresh

ORDER BY sort_order, schema_name, table_name
