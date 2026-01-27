{% macro apply_snowflake_tag_if_tagged(dbt_tag, sf_tag_name, sf_tag_value) %}
  {% if dbt_tag in config.get('tags', []) %}
    {% set object_type = config.get('materialized', 'table') %}
    {% if object_type == 'view' %}
      ALTER VIEW {{ this }} SET TAG {{ this.database }}.{{ this.schema }}.{{ sf_tag_name }} = '{{ sf_tag_value }}';
    {% else %}
      ALTER TABLE {{ this }} SET TAG {{ this.database }}.{{ this.schema }}.{{ sf_tag_name }} = '{{ sf_tag_value }}';
    {% endif %}
  {% endif %}
{% endmacro %}
