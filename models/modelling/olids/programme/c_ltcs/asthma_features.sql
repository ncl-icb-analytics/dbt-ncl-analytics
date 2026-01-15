{{
    config(
        materialized='table',
        tags=['cltcs'])    
}}

{% set asthma_code_list = dbt_utils.get_column_values(
    ref('raw_phenolab_aic_definitions'),
    'code',
    where="definition_name = 'asthma_SNOMED'"
) %}

{% set tests_code_list = dbt_utils.get_column_values(
    ref('asthma_testing_codelist'),
    'code',
    where="definition_name IN ('spirometry_SNOMED', 'peak_expiratory_flow_rate_SNOMED')"
    )
%}

{% set act_code_list = dbt_utils.get_column_values(
    ref('asthma_testing_codelist'),
    'code',
    where="definition_name = 'asthma_control_test_SNOMED'"
    )
%}

with persons as (

    select id
    from {{ ref('stg_olids_person') }}

),

diagnosis_no_testing_ids as (

    {{ get_persons_subset(
        inclusion_code_list = asthma_code_list,
        exclusion_code_list = tests_code_list,
        date_from = '2023-01-01'
    ) }}

),

testing_no_diagnosis_ids as (

    {{ get_persons_subset(
        inclusion_code_list = tests_code_list,
        exclusion_code_list = asthma_code_list,
        date_from = '2023-01-01'
    ) }}

),

diagnosis_no_act_ids as (

    {{ get_persons_subset(
        inclusion_code_list = asthma_code_list,
        exclusion_code_list = act_code_list,
        date_from = '2023-01-01'
    ) }}

)

select
    p.id,

    case 
        when d.person_id is not null then true
        else false
    end as testing_no_diagnosis,

    case 
        when t.person_id is not null then true
        else false
    end as diagnosis_no_testing,

    case 
        when act.person_id is not null then true
        else false
    end as diagnosis_no_act

from persons p
left join diagnosis_no_testing_ids d
    on p.id = d.person_id
left join testing_no_diagnosis_ids t
    on p.id = t.person_id
left join diagnosis_no_act_ids act
    on p.id = act.person_id