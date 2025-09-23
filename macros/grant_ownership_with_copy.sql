{% macro grant_ownership_with_copy(role='ANALYST') %}
  {% if config.get('materialized') == 'ephemeral' %}
    {# Ephemeral models don't create database objects, so skip grants #}
  {% else %}
    {% set object_type = 'TABLE' %}

    {% if config.get('materialized') == 'view' %}
      {% set object_type = 'VIEW' %}
    {% elif config.get('materialized') == 'table' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'incremental' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'snapshot' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'seed' %}
      {% set object_type = 'TABLE' %}
    {% else %}
      {% set object_type = 'TABLE' %}
    {% endif %}

    GRANT OWNERSHIP ON {{ object_type }} {{ this }} TO ROLE {{ role }} COPY CURRENT GRANTS
  {% endif %}
{% endmacro %}