{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MHSDS_TALKING_THERAPIES \ndbt: source(''reference_analyst_managed'', ''MHSDS_TALKING_THERAPIES'') \nColumns:\n  MTH -> mth\n  REPORTING_PERIOD_START -> reporting_period_start\n  REPORTING_PERIOD_END -> reporting_period_end\n  GROUP_TYPE -> group_type\n  ORG_CODE1 -> org_code1\n  ORG_NAME1 -> org_name1\n  ORG_CODE2 -> org_code2\n  ORG_NAME2 -> org_name2\n  MEASURE_ID -> measure_id\n  MEASURE_NAME -> measure_name\n  MEASURE_VALUE_SUPPRESSED -> measure_value_suppressed"
    )
}}
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
