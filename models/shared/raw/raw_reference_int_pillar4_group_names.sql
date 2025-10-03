-- Raw layer model for reference_analyst_managed.INT_PILLAR4_GROUP_NAMES
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "P4_GROUP_KEY" as p4_group_key,
    "P4_GROUP_NAME" as p4_group_name,
    "LTC_COUNT" as ltc_count
from {{ source('reference_analyst_managed', 'INT_PILLAR4_GROUP_NAMES') }}
