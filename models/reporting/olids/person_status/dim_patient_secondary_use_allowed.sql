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

-- int_patient_person_unique is sk_patient_id × person_id (not unique on sk_patient_id —
-- ~400k sk_patient_ids map to multiple persons). Carrying person_id through the
-- SELECT would fan out the rows; for an "is allowed?" lookup keyed by sk_patient_id
-- we only need to know that AT LEAST ONE person sharing this sk_patient_id is
-- allowed for secondary use. DISTINCT on sk_patient_id alone gives that grain.
SELECT DISTINCT
    pp.sk_patient_id,
    TRUE AS is_allowed_secondary_use
FROM {{ ref('int_patient_person_unique') }} pp
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} d
    ON pp.person_id = d.person_id
