{% macro ensure_governance_tags() %}
  {% if execute %}
    {# Schemas needing the DATA_CATEGORY tag: any model carrying a secondary-use governance tag (opt-out filtered OR s251-exempt). #}
    {% set data_category_schemas = [] %}
    {# Schemas needing the S251_EXEMPT tag: only models published under a Section 251 exemption. #}
    {% set s251_exempt_schemas = [] %}

    {% for node in graph.nodes.values() %}
      {% if node.resource_type in ['model', 'snapshot'] %}
        {% set key = node.database ~ '.' ~ node.schema %}
        {% if 'secondary_use_opt_out' in node.tags or 'secondary_use_s251_exempt' in node.tags %}
          {% if key not in data_category_schemas %}
            {% do data_category_schemas.append(key) %}
          {% endif %}
        {% endif %}
        {% if 'secondary_use_s251_exempt' in node.tags %}
          {% if key not in s251_exempt_schemas %}
            {% do s251_exempt_schemas.append(key) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% for key in data_category_schemas %}
      {% set parts = key.split('.') %}
      {% set database = parts[0] %}
      {% set schema = parts[1] %}
      {% set sql %}
        CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};
        CREATE TAG IF NOT EXISTS {{ database }}.{{ schema }}.DATA_CATEGORY;
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}

    {% for key in s251_exempt_schemas %}
      {% set parts = key.split('.') %}
      {% set database = parts[0] %}
      {% set schema = parts[1] %}
      {% set sql %}
        CREATE TAG IF NOT EXISTS {{ database }}.{{ schema }}.S251_EXEMPT;
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
