{% macro log_row_count() %}
  {% if execute %}
    {% set materialization = config.get('materialized') %}
    {% if target.name in ['prod', 'snowflake-prod'] and materialization in ('table', 'incremental') %}
      INSERT INTO DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG
        (model_name, database_name, schema_name, row_count, run_started_at, invocation_id)
      SELECT
        '{{ this.identifier }}',
        '{{ this.database }}',
        '{{ this.schema }}',
        COUNT(*),
        '{{ run_started_at }}'::TIMESTAMP_NTZ,
        '{{ invocation_id }}'
      FROM {{ this }}
    {% else %}
      SELECT 1 WHERE FALSE
    {% endif %}
  {% endif %}
{% endmacro %}
