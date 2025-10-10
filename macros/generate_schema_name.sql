{#
    Schema name generation macro

    Environments (dev, test, prod) are handled at the database level instead of the schema level.

    Automatic schema derivation:
    - For domains listed in var('auto_schema_domains'), schema names are automatically derived from subdomain folder structure
    - Pattern: {DOMAIN}_{SUBDOMAIN}
    - Examples:
      - models/modelling/olids/diagnoses/ → OLIDS_DIAGNOSES schema
      - models/reporting/olids/person_analytics/ → OLIDS_PERSON_ANALYTICS schema
    - Configure which domains use automatic schema naming via the 'auto_schema_domains' variable in dbt_project.yml
    - Other domains use explicit schema configuration from dbt_project.yml

    Custom schemas:
    - When a custom schema is explicitly set in dbt_project.yml, that value is used instead
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# If no custom schema, derive from folder structure #}
        {%- set path_parts = node.fqn -%}
        {%- set auto_schema_domains = var('auto_schema_domains', []) -%}

        {# Check if model is in modelling or reporting with subdomain folders and domain uses auto schema #}
        {%- if path_parts | length >= 4 and path_parts[1] in ['modelling', 'reporting'] and path_parts[2] in auto_schema_domains -%}
            {# Extract domain and subdomain from folder structure #}
            {%- set domain = path_parts[2] | upper -%}
            {%- set subdomain = path_parts[3] | upper -%}
            {{ domain ~ '_' ~ subdomain }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}

    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}