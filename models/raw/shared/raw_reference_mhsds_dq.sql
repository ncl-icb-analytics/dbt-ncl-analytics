-- Raw layer model for reference_analyst_managed.MHSDS_DQ
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "REPORTING_PERIOD" as reporting_period,
    "IS_LATEST" as is_latest,
    "CDP_MEASURE_ID" as cdp_measure_id,
    "CDP_MEASURE_NAME" as cdp_measure_name,
    "ORG_TYPE" as org_type,
    "ORG_CODE" as org_code,
    "ORG_NAME" as org_name,
    "ICB_CODE" as icb_code,
    "ICB_NAME" as icb_name,
    "REGION_CODE" as region_code,
    "REGION_NAME" as region_name,
    "MEASURE_TYPE" as measure_type,
    "MEASURE_VALUE" as measure_value,
    "STANDARD" as standard,
    "LTP_TRAJECTORY" as ltp_trajectory,
    "LTP_TRAJECTORY_PERCENTAGE_ACHIEVED" as ltp_trajectory_percentage_achieved,
    "PLAN" as plan,
    "PLAN_PERCENTAGE_ACHIEVED" as plan_percentage_achieved,
    "MEASURE_VALUE_STR" as measure_value_str,
    "STANDARD_STR" as standard_str,
    "LTP_TRAJECTORY_STR" as ltp_trajectory_str,
    "LTP_TRAJECTORY_PERCENTAGE_ACHIEVED_STR" as ltp_trajectory_percentage_achieved_str,
    "PLAN_STR" as plan_str,
    "PLAN_PERCENTAGE_ACHIEVED_STR" as plan_percentage_achieved_str,
    "LAST_MODIFIED" as last_modified
from {{ source('reference_analyst_managed', 'MHSDS_DQ') }}
