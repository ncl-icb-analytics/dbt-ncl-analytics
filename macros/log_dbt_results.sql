-- macros/log_dbt_results.sql

{% macro log_dbt_results() %}
  {% if execute %}

    {# --- Ensure schema exists --- #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS COMMISSIONING_MODELLING") %}

    {# --- Create run results table if not exists --- #}
    {% do run_query("""
      CREATE TABLE IF NOT EXISTS COMMISSIONING_MODELLING.DBT_OBSERVABILITY_RUNS (
        run_id STRING,
        invocation_id STRING,
        execution_time TIMESTAMP,
        status STRING,
        model_name STRING,
        rows_affected NUMBER,
        execution_time_seconds FLOAT
      );
    """) %}

    {% set results_sql %}
      INSERT INTO COMMISSIONING_MODELLING.DBT_OBSERVABILITY_RUNS
      SELECT
        '{{ invocation_id }}' AS run_id,
        '{{ invocation_id }}' AS invocation_id,
        CURRENT_TIMESTAMP() AS execution_time,
        '{{ run_results | map(attribute='status') | list }}' AS status,
        model_name,
        rows_affected,
        execution_time
      FROM (VALUES
        {% for result in results %}
          ('{{ result.node.name }}', '{{ result.status }}', {{ result.adapter_response.rows_affected or 0 }}, {{ result.execution_time }})
          {% if not loop.last %},{% endif %}
        {% endfor %}
      ) AS t(model_name, status, rows_affected, execution_time)
    {% endset %}

    {% do run_query(results_sql) %}

  {% endif %}
{% endmacro %}
