-- Raw layer model for reference_analyst_managed.UEC__DAILY__SMART
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "RESPONSE_CHILD_ID" as response_child_id,
    "VALUE" as value,
    "DATE_DATA" as date_data,
    "DATETIME_UPLOADED" as datetime_uploaded,
    "SITE_CODE_SMART" as site_code_smart,
    "SITE_CODE" as site_code,
    "SITE_NAME" as site_name,
    "INDICATOR_NAME" as indicator_name,
    "INDICATOR_KEY_NAME" as indicator_key_name,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__SMART') }}
