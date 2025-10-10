-- Raw layer model for reference_analyst_managed.UEC__DAILY__TRACKER_DAILY_DELAY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE_DATA" as date_data,
    "PROVIDER_CODE" as provider_code,
    "METRIC_NAME" as metric_name,
    "METRIC_VALUE" as metric_value,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_DAILY_DELAY') }}
