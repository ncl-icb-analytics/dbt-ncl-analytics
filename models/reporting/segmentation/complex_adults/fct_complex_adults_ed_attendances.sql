{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Type 1 A&E attendances in the last 12 months for complex adults cohort members.
-- One row per attendance. Window is rolling from the build date.

SELECT
    c.person_id,
    e.sk_patient_id,
    e.visit_occurrence_id,
    e.start_date AS arrival_date,
    ec.attendance_arrival_time AS arrival_time,
    e.end_date AS departure_date,
    ec.attendance_departure_time AS departure_time,
    e.organisation_id AS provider_code,
    e.organisation_name AS provider_name,
    e.site_id,
    e.site_name,
    e.chief_complaint_code,
    e.chief_complaint_desc,
    e.primary_diagnosis_code_snomed,
    e.primary_diagnosis_desc_snomed
FROM {{ ref('int_sus_uec_encounter') }} AS e
INNER JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON e.sk_patient_id = c.sk_patient_id
LEFT JOIN {{ ref('stg_sus_ae_emergency_care') }} AS ec
    ON e.visit_occurrence_id = ec.primarykey_id
WHERE e.pod = 'AE-T1'
    AND e.start_date >= DATEADD(MONTH, -12, CURRENT_DATE())
