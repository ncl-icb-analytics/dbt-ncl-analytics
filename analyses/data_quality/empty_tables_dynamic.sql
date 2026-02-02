/*
  Empty Tables Report (Dynamic dbt Query)

  Uses dbt's graph object to dynamically query all table-materialized models
  and check their row counts.

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

WITH row_counts AS (
    {%- for model in table_models | sort(attribute='name') -%}
        {%- set relation = model.database ~ '.' ~ model.schema ~ '.' ~ (model.alias or model.name) %}

    SELECT
        '{{ model.name }}' AS model_name,
        '{{ model.database }}.{{ model.schema }}' AS location,
        '{{ model.config.materialized }}' AS materialization,
        '{{ model.original_file_path }}' AS file_path,
        (SELECT COUNT(*) FROM {{ relation }}) AS row_count
        {%- if not loop.last %}

    UNION ALL
        {%- endif -%}
    {%- endfor %}
)

SELECT
    model_name,
    location,
    materialization,
    file_path,
    row_count,
    CASE
        WHEN row_count = 0 THEN 'EMPTY TABLE'
        WHEN row_count < 10 THEN 'Very few rows'
        ELSE 'OK'
    END AS status,
    CASE
        WHEN row_count = 0 AND materialization = 'incremental'
            THEN 'Consider running with --full-refresh'
        WHEN row_count = 0
            THEN 'Check source data or filter conditions'
        ELSE NULL
    END AS recommendation
FROM row_counts
WHERE row_count = 0
ORDER BY location, model_name

{%- else -%}

SELECT 'No table or incremental models found' AS message

{%- endif -%}
