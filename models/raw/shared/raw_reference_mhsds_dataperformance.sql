{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MHSDS_DATAPERFORMANCE \ndbt: source(''reference_analyst_managed'', ''MHSDS_DATAPERFORMANCE'') \nColumns:\n  MTH -> mth\n  REPORTING_PERIOD_START -> reporting_period_start\n  REPORTING_PERIOD_END -> reporting_period_end\n  STATUS -> status\n  BREAKDOWN -> breakdown\n  PRIMARY_LEVEL -> primary_level\n  PRIMARY_LEVEL_DESCRIPTION -> primary_level_description\n  SECONDARY_LEVEL -> secondary_level\n  SECONDARY_LEVEL_DESCRIPTION -> secondary_level_description\n  MEASURE_ID -> measure_id\n  MEASURE_NAME -> measure_name\n  MEASURE_VALUE -> measure_value"
    )
}}
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
