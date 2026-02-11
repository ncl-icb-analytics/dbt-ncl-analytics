-- macros/log_test_results.sql

{% macro log_test_results() %}
  {% if execute %}

    {# --- Ensure schema exists --- #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS COMMISSIONING_MODELLING") %}

    {# --- Create test results table if not exists --- #}
    {% do run_query("""
      CREATE TABLE IF NOT EXISTS COMMISSIONING_MODELLING.DBT_OBSERVABILITY_TESTS (
        invocation_id STRING,
        test_name STRING,
        test_type STRING,
        model_name STRING,
        column_name STRING,
        executed_at TIMESTAMP,
        status STRING,
        failures_count NUMBER,
        execution_time_seconds FLOAT
      );
    """) %}

    {% set test_results = results | selectattr('node.resource_type', 'equalto', 'test') | list %}
    {% if test_results | length > 0 %}

      {% set insert_sql %}
        INSERT INTO COMMISSIONING_MODELLING.DBT_OBSERVABILITY_TESTS (
          invocation_id,
          test_name,
          test_type,
          model_name,
          column_name,
          executed_at,
          status,
          failures_count,
          execution_time_seconds
        )
        VALUES
        {% for result in test_results %}
          (
            '{{ invocation_id }}',
            '{{ result.node.unique_id }}',
            '{{ result.node.test_metadata.name if result.node.test_metadata else "custom" }}',
            '{{ result.node.depends_on.nodes[0].split(".")[-1] if result.node.depends_on.nodes else "" }}',
            '{{ result.node.column_name if result.node.column_name else "" }}',
            CURRENT_TIMESTAMP(),
            '{{ result.status }}',
            {{ result.failures if result.failures else 0 }},
            {{ result.execution_time }}
          )
          {% if not loop.last %},{% endif %}
        {% endfor %}
      {% endset %}

      {% do run_query(insert_sql) %}
    {% endif %}
  {% endif %}
{% endmacro %}
