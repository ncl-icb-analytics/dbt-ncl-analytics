{{ config(materialized='table') }}

-- Intermediate model for LTC LCS RS Base Population
-- Reusable base population for LTC LCS register stratification.
-- Matches the EMIS LTC LCS Base by taking the union of the LTC register populations.

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
