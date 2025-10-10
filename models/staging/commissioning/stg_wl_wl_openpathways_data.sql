{{
    config(materialized = 'view')
}}

SELECT
    Pseudo_NHS_NUMBER as sk_patient_id,
    organisation_identifier_code_of_provider as provider_code,
    activity_treatment_function_code as tfc_code,
    organisation_identifier_code_of_commissioner as commissioner_code,
    der_LSOA2021 as lsoa_2021,
    referral_request_received_date,
    referral_to_treatment_period_start_date,
    -- Correct all dates to Sundays (Snowflake syntax)
    CASE
        WHEN DAYOFWEEKISO(Week_Ending_Date) = 7 THEN Week_Ending_Date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(Week_Ending_Date), Week_Ending_Date)  -- Move to previous Sunday
    END AS snapshot_date,
    1 AS open_pathways
FROM {{ ref('raw_wl_wl_openpathways_data') }}
WHERE Week_Ending_Date IS NOT NULL
    AND Week_Ending_Date <= CURRENT_DATE