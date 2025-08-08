{{ config(materialized='table') }}


SELECT
    patient_id,
    COUNT(*) AS wl_current_total_count
FROM {{ ref('int_wl_current') }}
GROUP BY patient_id

