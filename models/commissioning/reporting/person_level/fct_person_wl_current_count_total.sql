{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient, number of providers and number of tfcs that these are recorded against.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    patient_id,
    COUNT(*) AS wl_current_total_count,
    COUNT(DISTINCT provider_code) AS wl_current_distinct_providers_count,
    COUNT(DISTINCT tfc_code) AS wl_current_distinct_tfc_count
FROM {{ ref('int_wl_current') }}
GROUP BY patient_id

