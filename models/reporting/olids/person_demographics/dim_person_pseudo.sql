{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'pseudonym'],
        cluster_by=['person_id'])
}}

/*
Person Pseudonym Dimension Table

Provides pseudonymized hex keys for patient re-identification.
Generates HxFlake format from sk_patient_id using standardized transformation.

Key Features:

• One row per person_id

• HxFlake format: XX-XXX-XXX (10 characters total)

• Reversible transformation for re-identification

• Links to person and patient records

• Note: Multiple person_ids may share the same hx_flake when they share the same sk_patient_id

Data Source: sk_patient_id from stg_olids_patient via person relationships
*/

SELECT
    -- Core Identifiers
    bd.person_id,
    bd.sk_patient_id,

    -- HxFlake Pseudonym Generation (exact formula match)
    {{ hxflake_pseudo_generation('bd.sk_patient_id') }} AS hx_flake

FROM {{ ref('dim_person_birth_death') }} bd
WHERE bd.sk_patient_id IS NOT NULL
ORDER BY bd.person_id