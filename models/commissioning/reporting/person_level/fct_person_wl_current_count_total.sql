{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient, number of waiting lists at unique providers, 
number of waiting lists under unique TFCs, and a flag for whether the patient has an open waiting list for the same TFC under multiple providers.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH DUPLICATE_TFCS AS (SELECT DISTINCT
                        patient_id,
                        tfc_code,
                        provider_code,
                        ROW_NUMBER() OVER(PARTITION BY patient_id, tfc_code ORDER BY provider_code) AS TFC_MULTIPLE_PROVIDERS_ROW_NUMBER
                        FROM {{ ref('int_wl_current') }}
                        WHERE patient_id IS NOT NULL),
DUPLICATE_TFCS_GROUPED AS (SELECT 
                           patient_id, 
                           MAX(tfc_multiple_providers_row_number) AS TFC_MULTIPLE_PROVIDERS_MAX_ROW_NUMBER
                           FROM DUPLICATE_TFCS
                           GROUP BY 
                           patient_id
                           )
SELECT
    wl.patient_id,
    COUNT(*) AS wl_current_total_count,
    COUNT(DISTINCT wl.provider_code) AS wl_current_distinct_providers_count,
    COUNT(DISTINCT wl.tfc_code) AS wl_current_distinct_tfc_count,
    CASE WHEN MAX(dtfc.tfc_multiple_providers_max_row_number) > 1 THEN TRUE ELSE FALSE END AS same_tfc_multiple_providers_flag
FROM {{ ref('int_wl_current') }} wl
LEFT JOIN DUPLICATE_TFCS_GROUPED dtfc ON wl.patient_id = dtfc.patient_id
WHERE wl.patient_id IS NOT NULL
GROUP BY wl.patient_id
