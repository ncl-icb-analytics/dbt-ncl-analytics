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
        NULL AS last_altered_timestamp
    FROM {{ ref('int_global_data_refresh_date') }}
),
table_refresh AS (
    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered::date AS refresh_date,
        last_altered AS last_altered_timestamp
    FROM {{ this.database }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name != 'METADATA_REFRESH_DATES'

    UNION ALL

    SELECT
        'table_refresh' AS metric_type,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name,
        last_altered::date AS refresh_date,
        last_altered AS last_altered_timestamp
    FROM {{ target.database.replace('PUBLISHED_REPORTING__SECONDARY_USE', 'REPORTING') }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema LIKE 'OLIDS_%'
)

SELECT * FROM global_refresh
UNION ALL
SELECT * FROM table_refresh
ORDER BY metric_type, database_name, schema_name, table_name
