-- Raw layer model for reference_analyst_managed.UEC__DAILY__REPORT_TARGETS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
