{% macro get_persons_subset(inclusion_code_list,
                            exclusion_code_list,
                            date_from) %}
    
    with

    ex as (
        select distinct person_id
        from {{ ref('stg_olids_observation') }}
        {% if exclusion_code_list %}
            where mapped_concept_code in {{ to_sql_list(exclusion_code_list) }}
            and clinical_effective_date >= '{{ date_from }}'
        {% else %}
            where 1 = 0
        {% endif %}
    ),

    inc as (
        select distinct person_id
        from {{ ref('stg_olids_observation') }}
        {% if inclusion_code_list %}
            where mapped_concept_code in {{ to_sql_list(inclusion_code_list) }}
            and clinical_effective_date >= '{{ date_from }}'
        {% else %}
            where 1 = 0
        {% endif %}
    )

    select person_id
    from inc
    where person_id not in (select person_id from ex)

{% endmacro %}