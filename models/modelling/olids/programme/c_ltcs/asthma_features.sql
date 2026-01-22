{{
    config(
        materialized='table',
        tags=['cltcs'])    
}}

{# 
   ----------------------------
   Code lists - get the snomed codes that are used to define our features
   ---------------------------- 
#}

{% 
    set asthma_code_list = dbt_utils.get_column_values(
    ref('raw_phenolab_aic_definitions'),
    'code',
    where="definition_name = 'asthma_SNOMED'"
) %}

{% 
    set tests_code_list = dbt_utils.get_column_values(
    ref('asthma_features_codelist'),
    'code',
    where="definition_name IN ('spirometry_SNOMED', 'peak_expiratory_flow_rate_SNOMED')"
    )
%}

{% 
    set act_code_list = dbt_utils.get_column_values(
    ref('asthma_features_codelist'),
    'code',
    where="definition_name = 'asthma_control_test_SNOMED'"
    )
%}

{%  
    set salbutamol_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'),
        'code',
        where="definition_name = 'salbutamol_inhaler_medication_SNOMED'"
    )
%}

{% 
    set preventor_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'),
        'code',
        where="definition_name = 'non_salbutamol_inhaler_medication_SNOMED'"
    )
%}

{%
    set date_from = "ADD_YEARS(CURRENT_DATE, -3), 'YYYY-MM-DD"
%}



{# 
   ----------------------------
   person ids - get person ids that match the inclusion/exclusion criteria
   ---------------------------- 
#}

with persons as (

    select id
    from {{ ref('stg_olids_person') }}

),

diagnosis_no_testing_ids as (
     -- Persons with asthma diagnosis but no spirometry / PEFR testing
    {{ get_olids_obs_persons_subset(
        inclusion_code_list = asthma_code_list,
        exclusion_code_list = tests_code_list,
        date_from = {{ date_from }}
    ) }}

),

testing_no_diagnosis_ids as (
    -- Persons with spirometry / PEFR testing but no asthma diagnosis
    {{ get_olids_obs_persons_subset(
        inclusion_code_list = tests_code_list,
        exclusion_code_list = asthma_code_list,
        date_from = {{ date_from }}
    ) }}

),

diagnosis_no_act_ids as (
     -- Persons with asthma diagnosis but no asthma control test
    {{ get_olids_obs_persons_subset(
        inclusion_code_list = asthma_code_list,
        exclusion_code_list = act_code_list,
        date_from = {{ date_from }}
    ) }}

),

salbutamol_only_ids as (
    -- Persons with only salbutamol prescriptions (no preventer medications)
    {{ get_olids_obs_persons_subset(
        inclusion_code_list = salbutamol_code_list,
        exclusion_code_list = preventor_code_list,
        date_from = {{ date_from }}
    ) }}
),

salbutamol_repeats_id as (
    -- Persons with only > 3 salbutamol prescriptions in 12 months
    select distinct person_id
    from {{ ref('int_asthma_medications_12m') }}
    where mapped_concept_code in {{ to_sql_list(salbutamol_code_list) }}
    and recent_order_count_12m > 3
)

{# 
   ----------------------------
   generate feature flags - build boolean feature flags for every person in olids
   ---------------------------- 
#}

select
    p.id as person_id,

    case 
        when dnt.person_id is not null then true
        else false
    end as testing_no_diagnosis,

    case 
        when tnd.person_id is not null then true
        else false
    end as diagnosis_no_testing,

    case 
        when act.person_id is not null then true
        else false
    end as diagnosis_no_act,

    case 
        when so.person_id is not null then true
        else false
    end as salbutamol_only,

    case 
        when sr.person_id is not null then true
        else false
    end as salbutamol_repeats

from persons p
left join diagnosis_no_testing_ids dnt
    on p.id = dnt.person_id
left join testing_no_diagnosis_ids tnd
    on p.id = tnd.person_id
left join diagnosis_no_act_ids act
    on p.id = act.person_id
left join salbutamol_only_ids so
    on p.id = so.person_id   
left join salbutamol_repeats_id sr
    on p.id = sr.person_id 