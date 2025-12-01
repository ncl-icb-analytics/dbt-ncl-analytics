{% macro clean_organisation_id(column_name, org_type_id=41) %}
    case 
        when {{ column_name }} in (
            select organisation_code 
            from {{ ref('stg_dictionary_dbo_organisation') }} 
            where SK_ORGANISATION_TYPE_ID = {{ org_type_id }}
        )
        then {{ column_name }}
        else left({{ column_name }}, 3)
    end
{% endmacro %}