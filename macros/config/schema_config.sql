/*
Schema Generation Configuration

Controls how schema names are automatically derived from folder structure.

Domains listed in auto_schema_domains will have schema names automatically 
derived from subdomain folder structure.

Pattern: {DOMAIN}_{SUBDOMAIN} 
Example: models/modelling/olids/diagnoses/ → OLIDS_DIAGNOSES

Usage in generate_schema_name.sql:
  {% if domain in get_auto_schema_domains() %}
*/

{% macro get_auto_schema_domains() %}
    {#-
    Returns list of domains that should have auto-generated schema names.
    Override via dbt var: --vars '{"auto_schema_domains": ["olids", "commissioning"]}'
    -#}
    {{ return(var('auto_schema_domains', ['olids'])) }}
{% endmacro %}

{% macro get_dbt_audit_schema() %}
    {#-
    Returns the schema name for dbt test audit/failure storage.
    Override via dbt var: --vars '{"dbt_audit_schema": "my_audit_schema"}'
    -#}
    {{ return(var('dbt_audit_schema', 'test_audit')) }}
{% endmacro %}
