/*
Elementary Observability Configuration

Controls Elementary package behavior across environments.
Artifacts and results are typically only uploaded in production.

Usage:
  These macros are typically called via dbt_project.yml vars section,
  but can also be used directly in model configs.
*/

{% macro is_elementary_enabled() %}
    {#-
    Returns whether Elementary should be enabled for the current target.
    Elementary is enabled only in production environments.
    -#}
    {{ return(target.name in ['prod', 'snowflake-prod']) }}
{% endmacro %}

{% macro should_disable_dbt_artifacts_autoupload() %}
    {#-
    Returns whether to disable artifact upload for non-prod targets.
    Artifacts are only uploaded in production to avoid cluttering dev environments.
    -#}
    {{ return(target.name not in ['prod', 'snowflake-prod']) }}
{% endmacro %}

{% macro should_disable_run_results() %}
    {#-
    Returns whether to disable run results collection for non-prod targets.
    -#}
    {{ return(target.name not in ['prod', 'snowflake-prod']) }}
{% endmacro %}

{% macro should_disable_tests_results() %}
    {#-
    Returns whether to disable test results collection for non-prod targets.
    -#}
    {{ return(target.name not in ['prod', 'snowflake-prod']) }}
{% endmacro %}

{% macro should_disable_dbt_invocation_autoupload() %}
    {#-
    Returns whether to disable invocation upload for non-prod targets.
    -#}
    {{ return(target.name not in ['prod', 'snowflake-prod']) }}
{% endmacro %}
