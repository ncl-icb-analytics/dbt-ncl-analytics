{{
    config(
        materialized='table',
        static_analysis='off')
}}


/*
Number of open pathways at NCL providers per patient.
Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

WITH PROVIDER_COUNTS AS (
    SELECT
    patient_id,
    provider_code,
    COALESCE(open_pathways, 0) AS open_pathways
    FROM {{ ref('int_wl_current') }}
    WHERE patient_id IS NOT NULL
)
SELECT
*
FROM PROVIDER_COUNTS wl
PIVOT
(
    SUM(open_pathways) FOR provider_code IN (
        SELECT DISTINCT
        provider_code
        FROM DEV__MODELLING.LOOKUP_NCL.PROVIDER_SHORTHAND
        )
) AS pvt