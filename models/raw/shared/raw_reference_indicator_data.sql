-- Raw layer model for reference_fingertips.INDICATOR_DATA
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "Indicator ID" as indicator_id,
    "Indicator Name" as indicator_name,
    "Parent Code" as parent_code,
    "Parent Name" as parent_name,
    "Area Code" as area_code,
    "Area Name" as area_name,
    "Area Type" as area_type,
    "AREA_ID" as area_id,
    "Gender" as gender,
    "Age" as age,
    "Category Type" as category_type,
    "Category" as category,
    "Time period" as time_period,
    "Value" as value,
    "Lower CI 95.0 limit" as lower_ci_95_0_limit,
    "Upper CI 95.0 limit" as upper_ci_95_0_limit,
    "Lower CI 99.8 limit" as lower_ci_99_8_limit,
    "Upper CI 99.8 limit" as upper_ci_99_8_limit,
    "Count" as count,
    "Denominator" as denominator,
    "Value note" as value_note,
    "Recent Trend" as recent_trend,
    "Compared to England value or percentiles" as compared_to_england_value_or_percentiles,
    "Compared to percentiles" as compared_to_percentiles,
    "Time period Sortable" as time_period_sortable,
    "New data" as new_data,
    "Compared to goal" as compared_to_goal,
    "Time period range" as time_period_range,
    "DATE_UPDATED_LOCAL" as date_updated_local,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INDICATOR_DATA') }}
