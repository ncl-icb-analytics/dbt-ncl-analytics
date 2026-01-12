{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UEC__DAILY__REPORT_TARGETS \ndbt: source(''reference_analyst_managed'', ''UEC__DAILY__REPORT_TARGETS'') \nColumns:\n  ID -> id\n  DATASET -> dataset\n  METRIC_NAME -> metric_name\n  TARGET_DESC -> target_desc\n  TARGET_VALUE -> target_value\n  TARGET_DIRECTION -> target_direction\n  TARGET_LENIENCE_AMOUNT -> target_lenience_amount\n  TARGET_LENIENCE_TYPE -> target_lenience_type\n  ACTIVE_FROM -> active_from\n  ACTIVE_UNTIL -> active_until\n  SITE -> site"
    )
}}
select
    "ID" as id,
    "DATASET" as dataset,
    "METRIC_NAME" as metric_name,
    "TARGET_DESC" as target_desc,
    "TARGET_VALUE" as target_value,
    "TARGET_DIRECTION" as target_direction,
    "TARGET_LENIENCE_AMOUNT" as target_lenience_amount,
    "TARGET_LENIENCE_TYPE" as target_lenience_type,
    "ACTIVE_FROM" as active_from,
    "ACTIVE_UNTIL" as active_until,
    "SITE" as site
from {{ source('reference_analyst_managed', 'UEC__DAILY__REPORT_TARGETS') }}
