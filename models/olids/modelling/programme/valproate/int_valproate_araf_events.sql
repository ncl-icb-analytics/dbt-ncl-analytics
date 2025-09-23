{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF-related events for each person, using mapped concepts, observation, and valproate program codes. Applies lookback logic as defined in valproate program codes.') }}

SELECT
    pp.person_id,
    o.clinical_effective_date AS araf_event_date,
    o.id AS araf_ID,
    o.mapped_concept_code AS araf_concept_code,
    o.mapped_concept_display AS araf_concept_display,
    vpc.code_category AS araf_code_category,
    coalesce (vpc.code_category = 'ARAF', FALSE)
        AS is_specific_araf_form_code
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON o.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE
    vpc.code_category = 'ARAF'
    AND (
        vpc.lookback_years_offset IS NULL
        OR o.clinical_effective_date
        >= dateadd(YEAR, vpc.lookback_years_offset, current_date())
    )
