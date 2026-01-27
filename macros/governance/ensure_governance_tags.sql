{% macro ensure_governance_tags() %}
  {% if execute %}
    {% set schema_keys = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type in ['model', 'snapshot'] and 'secondary_use_opt_out' in node.tags %}
        {% set key = node.database ~ '.' ~ node.schema %}
        {% if key not in schema_keys %}
          {% do schema_keys.append(key) %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% for key in schema_keys %}
      {% set parts = key.split('.') %}
      {% set database = parts[0] %}
      {% set schema = parts[1] %}
      {% set sql %}
        CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};
        CREATE TAG IF NOT EXISTS {{ database }}.{{ schema }}.DATA_CATEGORY;
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
