{% macro get_persons_subset(inclusion_code_list,
                            exclusion_code_list,
                            date_from) %}

 {#
      Returns person_ids that:
        - have at least one observation with a code in inclusion_code_list
        - have no observations with a code in exclusion_code_list
        - only considers observations on or after date_from

      Parameters:
        inclusion_code_list (list[str]):
            Codes defining the inclusion criteria.
            If empty or null, no persons will be included.

        exclusion_code_list (list[str]):
            Codes defining the exclusion criteria.
            If empty or null, no persons will be excluded.

        date_from (string, YYYY-MM-DD):
            Earliest clinical_effective_date to consider.

      Output:
        A query returning a single column:
            - person_id
    #}
    
    with

    ex as (
        select distinct person_id
        from {{ ref('stg_olids_observation') }}
        {% if exclusion_code_list %}
            where mapped_concept_code in {{ to_sql_list(exclusion_code_list) }}
            and clinical_effective_date >= '{{ date_from }}'
        {% else %}
        --No exclusions gives the empty set
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
        --No inclusions gives the empty set
            where 1 = 0
        {% endif %}
    )

    select person_id
    from inc
    where person_id not in (select person_id from ex)

{% endmacro %}