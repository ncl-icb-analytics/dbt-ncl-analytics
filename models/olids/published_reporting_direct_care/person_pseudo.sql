{{
    config(
        materialized='view',
        tags=['published', 'direct_care', 'person', 'pseudonym'])
}}

/*
Published View: Person Pseudonym for Direct Care

Provides pseudonymized hex keys for patient re-identification in direct care contexts.
Built on dim_person_pseudo with HxFlake format transformation.

Key Features:

• One row per person_id

• HxFlake format: XX-XXX-XXX (10 characters total)

• Reversible transformation for re-identification

• Direct care access layer

Data Source: dim_person_pseudo
*/

SELECT
    person_id,
    sk_patient_id,
    hx_flake
FROM {{ ref('dim_person_pseudo') }}

ORDER BY person_id