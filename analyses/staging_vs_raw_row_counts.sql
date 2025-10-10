-- Row count comparison between staging and raw layers
-- Helps identify potential issues with deduplication, filtering, or joins
--
-- Usage: dbt compile -s staging_vs_raw_row_counts, then execute the compiled SQL in Snowflake

{%- set staging_models = [] -%}
{%- set raw_models = {} -%}

{#- Collect all staging and raw models -#}
{%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
    {%- if node.name.startswith('stg_') and 'staging' in node.fqn -%}
        {%- do staging_models.append(node) -%}
    {%- elif node.name.startswith('raw_') and 'raw' in node.fqn -%}
        {%- do raw_models.update({node.name: node}) -%}
    {%- endif -%}
{%- endfor -%}

{%- if staging_models | length > 0 %}

WITH staging_counts AS (
    {%- for stg_node in staging_models | sort(attribute='name') -%}
        {%- set stg_relation = stg_node.database ~ '.' ~ stg_node.schema ~ '.' ~ stg_node.alias %}

    SELECT '{{ stg_node.name }}' as model_name, COUNT(*) as row_count
    FROM {{ stg_relation }}
        {%- if not loop.last %}

    UNION ALL
        {%- endif -%}
    {%- endfor %}
),

raw_counts AS (
    {%- for stg_node in staging_models | sort(attribute='name') -%}
        {%- set expected_raw_name = stg_node.name.replace('stg_', 'raw_') -%}
        {%- if expected_raw_name in raw_models -%}
            {%- set raw_node = raw_models[expected_raw_name] -%}
            {%- set raw_relation = raw_node.database ~ '.' ~ raw_node.schema ~ '.' ~ raw_node.alias %}

    SELECT '{{ expected_raw_name }}' as model_name, COUNT(*) as row_count
    FROM {{ raw_relation }}
            {%- if not loop.last %}

    UNION ALL
            {%- endif -%}
        {%- endif -%}
    {%- endfor %}
)

SELECT
    sc.model_name as staging_model,
    REPLACE(sc.model_name, 'stg_', 'raw_') as expected_raw_model,
    CASE WHEN rc.model_name IS NOT NULL THEN '✓' ELSE '✗' END as raw_exists,
    sc.row_count as stg_rows,
    rc.row_count as raw_rows,
    sc.row_count - COALESCE(rc.row_count, 0) as row_diff,
    ROUND((sc.row_count::FLOAT / NULLIF(rc.row_count, 0)) * 100, 2) as pct_of_raw,
    CASE
        WHEN rc.model_name IS NULL THEN '⚠️ NO RAW TABLE FOUND'
        WHEN sc.row_count > rc.row_count THEN '❌ STAGING > RAW (possible joins?)'
        WHEN sc.row_count = 0 AND rc.row_count > 0 THEN '❌ STAGING EMPTY BUT RAW HAS DATA'
        WHEN sc.row_count < (rc.row_count * 0.5) THEN '⚠️ SIGNIFICANT DROP (>50% rows removed)'
        WHEN sc.row_count = rc.row_count THEN '✅ 1:1 PASSTHROUGH'
        ELSE '✅ DEDUPLICATION APPLIED'
    END as status_flag
FROM staging_counts sc
LEFT JOIN raw_counts rc
    ON REPLACE(sc.model_name, 'stg_', 'raw_') = rc.model_name
ORDER BY
    CASE WHEN rc.model_name IS NULL THEN 0 ELSE 1 END,  -- Missing raw tables first
    ABS(sc.row_count - COALESCE(rc.row_count, 0)) DESC  -- Then by largest differences
{%- else -%}

SELECT 'No staging models found' as message
{%- endif -%}
