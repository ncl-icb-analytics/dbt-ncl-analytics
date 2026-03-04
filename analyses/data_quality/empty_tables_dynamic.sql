/*
  Empty Tables Report (Dynamic dbt Query)

  Uses dbt's graph object to dynamically list all table-materialized models
  and checks row counts via each database's INFORMATION_SCHEMA.TABLES (avoids
  ACCOUNT_USAGE and missing-table errors).

  Usage: dbt compile -s empty_tables_dynamic, then run the compiled SQL in Snowflake
*/

{%- set table_models = [] -%}

{#- Collect all table and incremental models -#}
{%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
    {%- if node.config.materialized in ['table', 'incremental'] -%}
        {%- do table_models.append(node) -%}
    {%- endif -%}
{%- endfor -%}

{%- if table_models | length > 0 %}

WITH model_list AS (
    SELECT * FROM (VALUES
    {%- for model in table_models | sort(attribute='name') -%}
        ('{{ model.name }}', '{{ model.database }}', '{{ model.schema }}', '{{ model.alias or model.name }}', '{{ model.config.materialized }}', '{{ model.original_file_path | replace("\\", "/") }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
    ) AS t(model_name, database_name, schema_name, table_name, materialization, file_path)
),

{%- set model_dbs = table_models | map(attribute='database') | unique | list -%}
row_counts AS (
    {%- for db in model_dbs %}
    SELECT table_catalog, table_schema, table_name, row_count
    FROM {{ db }}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
      AND (table_catalog, table_schema, table_name) IN (
        {%- for model in table_models | selectattr('database', 'equalto', db) | sort(attribute='name') -%}
        ('{{ model.database }}', '{{ model.schema }}', '{{ model.alias or model.name }}'){% if not loop.last %},{% endif %}
        {%- endfor %}
      )
    {%- if not loop.last %}

    UNION ALL
    {%- endif %}
    {%- endfor %}
),

joined AS (
    SELECT
        m.model_name,
        m.database_name || '.' || m.schema_name AS location,
        m.materialization,
        m.file_path,
        r.row_count
    FROM model_list m
    LEFT JOIN row_counts r
        ON r.table_catalog = m.database_name
        AND r.table_schema = m.schema_name
        AND r.table_name = m.table_name
)

SELECT
    model_name,
    location,
    materialization,
    file_path,
    COALESCE(row_count, 0) AS row_count,
    CASE
        WHEN row_count IS NULL THEN 'MISSING TABLE'
        WHEN row_count = 0 THEN 'EMPTY TABLE'
        WHEN row_count < 10 THEN 'Very few rows'
        ELSE 'OK'
    END AS status,
    CASE
        WHEN row_count IS NULL
            THEN 'Table not found in Snowflake - run dbt build'
        WHEN row_count = 0 AND materialization = 'incremental'
            THEN 'Consider running with --full-refresh'
        WHEN row_count = 0
            THEN 'Check source data or filter conditions'
        ELSE NULL
    END AS recommendation
FROM joined
WHERE row_count IS NULL OR row_count = 0
ORDER BY location, model_name

{%- else -%}

SELECT 'No table or incremental models found' AS message

{%- endif -%}
