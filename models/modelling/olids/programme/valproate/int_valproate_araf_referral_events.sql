{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).') }}

SELECT
    pp.person_id,
    o.clinical_effective_date AS araf_referral_event_date,
    o.id AS araf_referral_ID,
    o.mapped_concept_code AS araf_referral_concept_code,
    o.mapped_concept_display AS araf_referral_concept_display,
    vpc.code_category AS araf_referral_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON o.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'REFERRAL'
