{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MHSDS_DQ \ndbt: source(''reference_analyst_managed'', ''MHSDS_DQ'') \nColumns:\n  REPORTING_PERIOD -> reporting_period\n  IS_LATEST -> is_latest\n  CDP_MEASURE_ID -> cdp_measure_id\n  CDP_MEASURE_NAME -> cdp_measure_name\n  ORG_TYPE -> org_type\n  ORG_CODE -> org_code\n  ORG_NAME -> org_name\n  ICB_CODE -> icb_code\n  ICB_NAME -> icb_name\n  REGION_CODE -> region_code\n  REGION_NAME -> region_name\n  MEASURE_TYPE -> measure_type\n  MEASURE_VALUE -> measure_value\n  STANDARD -> standard\n  LTP_TRAJECTORY -> ltp_trajectory\n  LTP_TRAJECTORY_PERCENTAGE_ACHIEVED -> ltp_trajectory_percentage_achieved\n  PLAN -> plan\n  PLAN_PERCENTAGE_ACHIEVED -> plan_percentage_achieved\n  MEASURE_VALUE_STR -> measure_value_str\n  STANDARD_STR -> standard_str\n  LTP_TRAJECTORY_STR -> ltp_trajectory_str\n  LTP_TRAJECTORY_PERCENTAGE_ACHIEVED_STR -> ltp_trajectory_percentage_achieved_str\n  PLAN_STR -> plan_str\n  PLAN_PERCENTAGE_ACHIEVED_STR -> plan_percentage_achieved_str\n  LAST_MODIFIED -> last_modified"
    )
}}
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
