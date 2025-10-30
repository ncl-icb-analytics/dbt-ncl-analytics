{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- if node.resource_type == 'seed' -%}
        {#- Seeds always use the literal database name without environment prefix -#}
        {%- if custom_database_name is none -%}
            {{ target.database }}
        {%- else -%}
            {{ custom_database_name | trim }}
        {%- endif -%}
    {%- else -%}
        {#- All other resources use environment-prefixed database names -#}
        {%- if target.name is none or target.name == "prod" -%}
            {%- set database_prefix = none -%}
        {%- else -%}
            {%- set database_prefix = target.name | upper | trim -%}
        {%- endif -%}

        {%- if custom_database_name is none -%}
            {%- if database_prefix is none -%}
                {{ target.database }}
            {%- else -%}
                {{ database_prefix }}__{{ target.database }}
            {%- endif -%}
        {%- else -%}
            {%- if database_prefix is none -%}
                {{ custom_database_name | trim }}
            {%- else -%}
                {{ database_prefix }}__{{ custom_database_name | trim }}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}