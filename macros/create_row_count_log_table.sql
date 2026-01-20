{% macro create_row_count_log_table() %}
  {% if target.name in ['prod', 'snowflake-prod'] %}
    CREATE TABLE IF NOT EXISTS DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG (
      model_name VARCHAR,
      database_name VARCHAR,
      schema_name VARCHAR,
      row_count NUMBER,
      run_started_at TIMESTAMP_NTZ,
      invocation_id VARCHAR,
      recorded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  {% endif %}
{% endmacro %}
