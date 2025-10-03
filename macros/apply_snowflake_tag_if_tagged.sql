{% macro apply_snowflake_tag_if_tagged(dbt_tag, sf_tag_name, sf_tag_value) %}
  {% if dbt_tag in config.get('tags', []) %}
    ALTER {{ this }} SET TAG {{ sf_tag_name }} = '{{ sf_tag_value }}';
  {% endif %}
{% endmacro %}
