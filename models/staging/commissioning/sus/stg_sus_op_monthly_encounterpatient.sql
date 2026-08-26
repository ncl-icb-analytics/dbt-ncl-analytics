{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    sk_gender_id,
    sk_ethnicity_id,
    sk_postcode_id,
    sk_practice_id,
    date_of_birth,
    age,
    sk_org_practice_id,
    ward_code,
    lsoa_code
from {{ ref('raw_sus_op_monthly_encounterpatient') }}
