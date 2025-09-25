{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'childhood_imms'],
        cluster_by=['person_id'])
}}



SELECT DISTINCT d.PERSON_ID
FROM {{ ref('stg_olids_observation') }} o
JOIN {{ ref('stg_reference_ecl_cache') }} ec ON o.mapped_concept_code = ec.code
JOIN {{ ref('int_patient_person_unique') }} ppu ON o.patient_id = ppu.patient_id
JOIN {{ ref('dim_person_demographics') }} d ON ppu.person_id = d.person_id
WHERE ec.cluster_id = 'LOOKED_AFTER_CHILD'
AND d.age < 25