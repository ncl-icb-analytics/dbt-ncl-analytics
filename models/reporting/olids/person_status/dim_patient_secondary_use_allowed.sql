{{
    config(
        materialized='table',
        tags=['dimension', 'patient', 'opt_out'],
        cluster_by=['sk_patient_id'],
        meta={
            'custom_message': 'Opt-out filter dim at sk_patient_id grain. INNER JOIN to this from OLIDS models keyed by sk_patient_id to apply National Data Opt-Out and Type 1 opt-out filtering. Fan-out of dim_person_secondary_use_allowed via int_patient_person_unique.'
        })
}}

/*
Patient-grain fan-out of dim_person_secondary_use_allowed.
One row per sk_patient_id for persons allowed for secondary use.
Use when the downstream model is keyed by sk_patient_id rather than person_id.
*/

SELECT DISTINCT
    pp.sk_patient_id,
    pp.person_id,
    TRUE AS is_allowed_secondary_use
FROM {{ ref('int_patient_person_unique') }} pp
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} d
    ON pp.person_id = d.person_id
