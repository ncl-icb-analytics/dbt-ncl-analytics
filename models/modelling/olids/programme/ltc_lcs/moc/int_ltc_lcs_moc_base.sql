{{ config(materialized='table') }}

-- LTC LCS Model of Care base population.
-- Union of all LTC LCS disease registers. Persons not on any of these registers
-- are excluded from downstream MOC / risk stratification reporting.

with register_population as (
    select person_id from {{ ref('fct_person_atrial_fibrillation_register') }}
    union
    select person_id from {{ ref('fct_person_ckd_register') }}
    union
    select person_id from {{ ref('fct_person_chd_register') }}
    union
    select person_id from {{ ref('fct_person_diabetes_register') }}
    union
    select person_id from {{ ref('fct_person_hypertension_register') }}
    union
    select person_id from {{ ref('fct_person_nafld_register') }}
    union
    select person_id from {{ ref('fct_person_asthma_register') }}
    union
    select person_id from {{ ref('fct_person_cyp_asthma_register') }}
    union
    select person_id from {{ ref('fct_person_copd_register') }}
    union
    select person_id from {{ ref('fct_person_heart_failure_register') }}
    union
    select person_id from {{ ref('fct_person_pad_register') }}
    union
    select person_id from {{ ref('fct_person_stroke_tia_register') }}
)

select distinct person_id
from register_population
