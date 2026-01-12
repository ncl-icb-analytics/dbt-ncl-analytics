{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UEC__DAILY__SMART \ndbt: source(''reference_analyst_managed'', ''UEC__DAILY__SMART'') \nColumns:\n  RESPONSE_CHILD_ID -> response_child_id\n  VALUE -> value\n  DATE_DATA -> date_data\n  DATETIME_UPLOADED -> datetime_uploaded\n  SITE_CODE_SMART -> site_code_smart\n  SITE_CODE -> site_code\n  SITE_NAME -> site_name\n  INDICATOR_NAME -> indicator_name\n  INDICATOR_KEY_NAME -> indicator_key_name\n  _TIMESTAMP -> timestamp"
    )
}}
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
