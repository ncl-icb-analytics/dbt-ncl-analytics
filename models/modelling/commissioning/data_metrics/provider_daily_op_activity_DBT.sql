--- provider_daily_op_activity_DBT.sql
--- Created by: J.Linney | dbt test file for OP provider daily activity

-- Add config overrides in here, eg. comment added to snowflake model metadata
-- and materialization type - although technically redundant here if declared in dbt_project.yml 
-- so just including it for clarity whilst testing

{{ config(
    materialized='view',
    post_hook=[
      "COMMENT ON VIEW {{ this }} IS 'Created by: J.Linney | dbt test file for OP provider daily activity'"
    ]
) }}

WITH base AS (
    SELECT
        appointment_commissioning_service_agreement_provider AS provider_code,
        DATE_TRUNC('day', appointment_date) AS activity_date
    FROM {{ ref('stg_sus_op_appointment') }}
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
WHERE b.activity_date >= DATEADD(day, -30, CURRENT_DATE)
AND b.provider_code IN ('RRV', 'RKE', 'RAP','RAL','RP4','RP6','RAN')
GROUP BY provider, activity_date
