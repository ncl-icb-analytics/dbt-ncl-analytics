{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    patient_id,
    COUNT(*) AS wl_current_total_count
FROM {{ ref('int_wl_current') }}
GROUP BY patient_id

