/*
Elementary Observability Configuration

These control Elementary package behavior - artifacts only uploaded in prod.
*/

{% macro is_elementary_enabled() %}
    {{- target.name in ['prod', 'snowflake-prod'] -}}
{% endmacro %}

{% macro should_disable_dbt_artifacts_autoupload() %}
    {{- target.name not in ['prod', 'snowflake-prod'] -}}
{% endmacro %}

{% macro should_disable_run_results() %}
    {{- target.name not in ['prod', 'snowflake-prod'] -}}
{% endmacro %}

{% macro should_disable_tests_results() %}
    {{- target.name not in ['prod', 'snowflake-prod'] -}}
{% endmacro %}

{% macro should_disable_dbt_invocation_autoupload() %}
    {{- target.name not in ['prod', 'snowflake-prod'] -}}
{% endmacro %}
