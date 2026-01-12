{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UEC__DAILY__TRACKER_DAILY_DELAY \ndbt: source(''reference_analyst_managed'', ''UEC__DAILY__TRACKER_DAILY_DELAY'') \nColumns:\n  DATE_DATA -> date_data\n  PROVIDER_CODE -> provider_code\n  METRIC_NAME -> metric_name\n  METRIC_VALUE -> metric_value\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "DATE_DATA" as date_data,
    "PROVIDER_CODE" as provider_code,
    "METRIC_NAME" as metric_name,
    "METRIC_VALUE" as metric_value,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_DAILY_DELAY') }}
