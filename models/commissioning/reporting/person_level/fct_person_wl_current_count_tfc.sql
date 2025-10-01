{{
    config(
        materialized='table',
        static_analysis='off')
}}


/*
Number of open pathways under TFCs per patient.
Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

WITH TFC_COUNTS AS (
    SELECT
    sk_patient_id,
    tfc_code,
    COALESCE(open_pathways, 0) AS open_pathways
    FROM {{ ref('int_wl_current') }}
    WHERE sk_patient_id IS NOT NULL
)
SELECT
*
FROM TFC_COUNTS wl
PIVOT
(
    SUM(open_pathways) FOR tfc_code IN (
        SELECT
        "BK_SpecialtyCode"
        from "Dictionary"."dbo"."Specialties"
        WHERE
        "IsTreatmentFunction" = TRUE
        )
) AS pvt