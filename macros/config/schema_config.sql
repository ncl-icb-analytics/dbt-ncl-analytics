/*
Schema Configuration

Usage:
  {% if domain in auto_schema_domains() %}
*/

{% macro auto_schema_domains() %}
    {{- var('auto_schema_domains', ['olids']) -}}
{% endmacro %}

{% macro dbt_audit_schema() %}
    {{- var('dbt_audit_schema', 'test_audit') -}}
{% endmacro %}
