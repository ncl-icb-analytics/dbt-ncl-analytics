-- Raw layer model for reference_analyst_managed.MHSDS_TALKING_THERAPIES
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "MTH" as mth,
    "REPORTING_PERIOD_START" as reporting_period_start,
    "REPORTING_PERIOD_END" as reporting_period_end,
    "GROUP_TYPE" as group_type,
    "ORG_CODE1" as org_code1,
    "ORG_NAME1" as org_name1,
    "ORG_CODE2" as org_code2,
    "ORG_NAME2" as org_name2,
    "MEASURE_ID" as measure_id,
    "MEASURE_NAME" as measure_name,
    "MEASURE_VALUE_SUPPRESSED" as measure_value_suppressed
from {{ source('reference_analyst_managed', 'MHSDS_TALKING_THERAPIES') }}
