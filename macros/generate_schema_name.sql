{#
    Schema name generation macro

    Environments (dev, test, prod) are handled at the database level instead of the schema level.

    Automatic schema derivation:
    - For OLIDS modelling/reporting subdomains, schema names are automatically derived from folder structure
    - Example: models/modelling/olids/diagnoses/ → OLIDS_DIAGNOSES schema
    - Example: models/reporting/olids/person_analytics/ → OLIDS_PERSON_ANALYTICS schema
    - This allows adding new subdomains without updating dbt_project.yml configuration

    Custom schemas:
    - When a custom schema is explicitly set in dbt_project.yml, that value is used instead
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# If no custom schema, derive from folder structure #}
        {%- set path_parts = node.fqn -%}

        {# Check if model is in olids modelling or reporting with subdomain folders #}
        {%- if path_parts | length >= 4 and path_parts[1] in ['modelling', 'reporting'] and path_parts[2] == 'olids' -%}
            {# Extract subdomain from folder structure (e.g., diagnoses, medications) #}
            {%- set subdomain = path_parts[3] | upper -%}
            {{ 'OLIDS_' ~ subdomain }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}

    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}