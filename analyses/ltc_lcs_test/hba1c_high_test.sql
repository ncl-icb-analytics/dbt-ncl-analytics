-- Test script for LTC LCS macros
-- Identifies patients with latest HbA1c > 90 mmol/mol
-- Uses valueset: on_dm_reg_pg1_hrc_vs1 (HbA1c level concepts)
-- Parent population: Diabetes register

with diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

latest_hba1c as (     -- find latest hba1c values for each person
    {{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs1") }}

)

select
    h.observation_id,
    h.person_id,
    h.clinical_effective_date,
    h.result_value,
    h.result_unit_display,
    h.concept_code,
    h.concept_display,
    h.valueset_friendly_name
from latest_hba1c h
join diabetes_register dr on h.person_id = dr.person_id
where h.result_value > 90