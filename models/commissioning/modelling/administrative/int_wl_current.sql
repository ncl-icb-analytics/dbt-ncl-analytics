{{
    config(
        materialized='table')
}}


/*
All active waiting lists.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

WITH most_recent_week AS (
    SELECT MAX(snapshot_date) AS max_date
    FROM {{ ref('stg_wl_wl_openpathways_data') }}
)

SELECT 
    dc.sk_patient_id,
    dc.provider_code,
    dc.tfc_code,
    dc.commissioner_code,
    dc.lsoa_2021,
    dc.referral_request_received_date,
    dc.referral_to_treatment_period_start_date,
    dc.snapshot_date,
    dc.open_pathways
FROM {{ ref('stg_wl_wl_openpathways_data') }} dc
INNER JOIN most_recent_week mrw ON dc.snapshot_date = mrw.max_date
