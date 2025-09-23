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

WITH date_corrected AS (
    SELECT
        Pseudo_NHS_NUMBER AS patient_id,
        organisation_identifier_code_of_provider AS provider_code,
        activity_treatment_function_code AS tfc_code,
        organisation_identifier_code_of_commissioner AS commissioner_code,
        der_LSOA2021 AS lsoa_2021,
        -- Correct all dates to Sundays (Snowflake syntax)
        CASE 
            WHEN DAYOFWEEK(Week_Ending_Date) = 1 THEN Week_Ending_Date  -- Already Sunday
            ELSE DATEADD('day', -(DAYOFWEEK(Week_Ending_Date) - 1), Week_Ending_Date)  -- Move to previous Sunday
        END AS snapshot_date,
        1 AS open_pathways
    FROM {{ ref('stg_wl_wl_openpathways_data') }}
    WHERE Week_Ending_Date IS NOT NULL
      AND Week_Ending_Date <= CURRENT_DATE
),
most_recent_week AS (
    SELECT MAX(snapshot_date) AS max_date
    FROM date_corrected
)
SELECT 
    dc.patient_id,
    dc.provider_code,
    dc.tfc_code,
    dc.commissioner_code,
    dc.lsoa_2021,
    dc.snapshot_date,
    dc.open_pathways
FROM date_corrected dc
CROSS JOIN most_recent_week mrw
WHERE dc.snapshot_date = mrw.max_date
