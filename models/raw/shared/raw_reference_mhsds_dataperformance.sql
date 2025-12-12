-- Raw layer model for reference_analyst_managed.MHSDS_DATAPERFORMANCE
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "MTH" as mth,
    "REPORTING_PERIOD_START" as reporting_period_start,
    "REPORTING_PERIOD_END" as reporting_period_end,
    "STATUS" as status,
    "BREAKDOWN" as breakdown,
    "PRIMARY_LEVEL" as primary_level,
    "PRIMARY_LEVEL_DESCRIPTION" as primary_level_description,
    "SECONDARY_LEVEL" as secondary_level,
    "SECONDARY_LEVEL_DESCRIPTION" as secondary_level_description,
    "MEASURE_ID" as measure_id,
    "MEASURE_NAME" as measure_name,
    "MEASURE_VALUE" as measure_value
from {{ source('reference_analyst_managed', 'MHSDS_DATAPERFORMANCE') }}
