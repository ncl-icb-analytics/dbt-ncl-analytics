{{
    config(
        materialized='view',
        alias='metadata_refresh_dates'
    )
}}

/*
OLIDS Data Refresh Metadata - Secondary Use

Provides centralised refresh date information for OLIDS data and dashboard tables
in the secondary use schema.

See metadata_refresh_dates in published_reporting_direct_care for full documentation.
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
        last_altered::date AS refresh_date,
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
        last_altered::date AS refresh_date,
        last_altered AS last_altered_timestamp,
        2 AS sort_order
    FROM {{ this.database.replace('PUBLISHED_REPORTING__SECONDARY_USE', 'REPORTING') }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema LIKE 'OLIDS_%'
),
modelling_tables AS (
    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered::date AS refresh_date,
        last_altered AS last_altered_timestamp,
        3 AS sort_order
    FROM {{ this.database.replace('PUBLISHED_REPORTING__SECONDARY_USE', 'MODELLING') }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema LIKE 'OLIDS_%'
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
FROM published_reporting_tables

UNION ALL

SELECT
    metric_type,
    database_name,
    schema_name,
    table_name,
    refresh_date,
    last_altered_timestamp,
    sort_order
FROM reporting_tables

UNION ALL

SELECT
    metric_type,
    database_name,
    schema_name,
    table_name,
    refresh_date,
    last_altered_timestamp,
    sort_order
FROM modelling_tables

ORDER BY sort_order, schema_name, table_name
