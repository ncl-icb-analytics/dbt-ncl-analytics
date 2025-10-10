-- Raw layer model for reference_analyst_managed.UEC__DAILY__LAS_HANDOVER
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE_DATA" as date_data,
    "SITE_NAME" as site_name,
    "SITE_CODE" as site_code,
    "CONVEYANCES_NONBLUE" as conveyances_nonblue,
    "CONVEYANCES_TOTAL_ED" as conveyances_total_ed,
    "TOTAL_HANDOVER" as total_handover,
    "ARRIVAL_TO_PATIENT_HANDOVER_AVERAGE" as arrival_to_patient_handover_average,
    "UNDER_15_MIN_COUNT" as under_15_min_count,
    "OVER_15_MIN_COUNT" as over_15_min_count,
    "OVER_15_MIN_PERCENT" as over_15_min_percent,
    "OVER_15_MIN_RATIO" as over_15_min_ratio,
    "OVER_15_MIN_PERCENT_RANK" as over_15_min_percent_rank,
    "OVER_15_MIN_OVERRUN_AVERAGE" as over_15_min_overrun_average,
    "TOTAL_TIME_LOST_OVER_15_MIN_HOURS" as total_time_lost_over_15_min_hours,
    "TOTAL_TIME_LOST_OVER_15_MIN_MINUTES" as total_time_lost_over_15_min_minutes,
    "OVER_30_MIN_COUNT" as over_30_min_count,
    "OVER_30_MIN_PERCENT" as over_30_min_percent,
    "TOTAL_TIME_LOST_OVER_30_MIN_HOURS" as total_time_lost_over_30_min_hours,
    "OVER_45_MIN_COUNT" as over_45_min_count,
    "OVER_45_MIN_PERCENT" as over_45_min_percent,
    "TOTAL_TIME_LOST_OVER_45_MIN_HOURS" as total_time_lost_over_45_min_hours,
    "OVER_60_MIN_COUNT" as over_60_min_count,
    "OVER_60_MIN_PERCENT" as over_60_min_percent,
    "TOTAL_TIME_LOST_OVER_60_MIN_HOURS" as total_time_lost_over_60_min_hours,
    "OVER_120_MIN_COUNT" as over_120_min_count,
    "OVER_120_MIN_PERCENT" as over_120_min_percent,
    "OVER_180_MIN_COUNT" as over_180_min_count,
    "OVER_180_MIN_PERCENT" as over_180_min_percent,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__LAS_HANDOVER') }}
