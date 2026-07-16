{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Non-elective admissions in the last 12 months for complex adults cohort members.
-- One row per spell (admission methods 2x, births excluded upstream; end dates
-- imputed for open/incomplete spells). Window is rolling from the build date.

SELECT
    c.person_id,
    s.sk_patient_id,
    s.visit_occurrence_id,
    s.start_date AS admission_date,
    s.start_time AS admission_time,
    s.end_date AS discharge_date,
    s.end_time AS discharge_time,
    s.duration AS length_of_stay_days,
    s.spell_admission_method AS admission_method,
    s.organisation_id AS provider_code,
    s.organisation_name AS provider_name
FROM {{ ref('int_sus_apc_imputed_spells') }} AS s
INNER JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON s.sk_patient_id = c.sk_patient_id
WHERE LEFT(s.spell_admission_method, 1) = '2'
    AND s.start_date >= DATEADD(MONTH, -12, CURRENT_DATE())
