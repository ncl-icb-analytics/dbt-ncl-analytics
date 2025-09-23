{{
    config(
        materialized='table')
}}


/*
Number of open pathways under TFCs per patient.
Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

WITH TFC_COUNTS AS (
    SELECT
    patient_id,
    tfc_code,
    open_pathways
    FROM {{ ref('int_wl_current') }}
    WHERE patient_id IS NOT NULL
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
    DEFAULT ON NULL (0)
) AS pvt