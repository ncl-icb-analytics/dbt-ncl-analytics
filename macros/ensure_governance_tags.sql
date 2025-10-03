{% macro ensure_governance_tags() %}
  {% if execute %}
    {% set schemas = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type in ['model', 'snapshot'] and 'secondary_use_opt_out' in node.tags %}
        {% do schemas.append({'database': node.database, 'schema': node.schema}) %}
      {% endif %}
    {% endfor %}

    {% for item in schemas | unique %}
      {% set sql %}
        CREATE SCHEMA IF NOT EXISTS {{ item.database }}.{{ item.schema }};
        CREATE TAG IF NOT EXISTS {{ item.database }}.{{ item.schema }}.DATA_CATEGORY;
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
