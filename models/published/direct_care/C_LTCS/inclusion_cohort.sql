{{
    config(
        materialized='table')
}}


/*
Inclusion criteria cohort for CLTCS

Clinical Purpose:
- Supporting the C-LTCS product by identifying patients with relevant conditions and recent outpatient activity

Testing:
- Actual table will use cambridge multimorbidity logic and filter to relevant appointments once finalised

*/
with conditions_inclusion as (
    select distinct person_id, 1 as has_condition
    from {{ ref('dim_person_conditions')}}
    where cardiovascular_conditions + respiratory_conditions + metabolic_conditions > 3
),
op_inclusion as(
    select distinct sk_patient_id, 1 as has_recent_op
    from {{ ref('fct_person_sus_op_recent')}}
    where op_att_tot_12mo > 2
)

select pp.sk_patient_id as patient_id
    ,pp.hx_flake as re_id_key
    ,pp.person_id as olids_id
    ,pd.practice_code
    ,pd.practice_name
    ,pd.pcn_code
    ,pd.pcn_name
    ,pd.main_language
    ,pd.age
    ,pd.gender
    ,pd.ethnicity_category
    ,case when pc.has_condition = 1 or op.has_recent_op = 1 then 1 else 0 end as eligible
from {{ref('dim_person_pseudo')}} pp
left join conditions_inclusion pc
    on pc.person_id = pp.person_id
left join op_inclusion op
    on op.sk_patient_id = pp.sk_patient_id
left join {{ref('dim_person_demographics')}} pd
    on pd.person_id = pp.person_id
where pd.is_deceased = FALSE 
    and pcn_code in (select distinct pcn_code from {{source('c_ltcs','MDT_LOOKUP')}} )