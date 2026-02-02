--- provider_daily_op_activity_DBT.sql
--- Created by: J.Linney | dbt test file for OP provider daily activity

-- Add config overrides in here, eg. comment added to snowflake model metadata
-- and materialisation type - although technically redundant here if declared in dbt_project.yml 
-- so just including it for clarity whilst testing
-- NOTE (18/12/25): Changed materialization now from view to table to as missing_summary test running too early on blank views, reulting in it being empty

{{ config(
    materialized='table',
    post_hook=[
      "COMMENT ON TABLE {{ this }} IS 'Created by: J.Linney | dbt test file for OP provider daily activity'"
    ]
) }}

WITH base AS (
    SELECT
        "appointment.commissioning.service_agreement.provider_derived" AS provider_code,
        DATE_TRUNC('day', "appointment.date") AS activity_date
    FROM {{ source('sus_unified_op','appointment') }}
),
provider_lookup AS (
    SELECT 
        REPORTING_CODE,
        PROVIDER_NAME
    FROM MODELLING.LOOKUP_NCL.NCL_PROVIDER
)
SELECT
    COALESCE(p.PROVIDER_NAME, b.provider_code) AS provider,
    b.activity_date,
    COUNT(*) AS records
FROM base b
LEFT JOIN provider_lookup p ON b.provider_code = p.REPORTING_CODE
WHERE b.activity_date >= DATEADD(day, -744, CURRENT_DATE)  -- 2 years monitoring window
--AND b.activity_date < DATEADD(day, -14, CURRENT_DATE)     -- Exclude last 2 weeks
AND b.provider_code IN ('RRV', 'RKE', 'RAP','RAL','RP4','RP6','RAN')
GROUP BY provider, activity_date
