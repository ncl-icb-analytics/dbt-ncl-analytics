{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.INDICATOR_DATA \ndbt: source(''reference_fingertips'', ''INDICATOR_DATA'') \nColumns:\n  Indicator ID -> indicator_id\n  Indicator Name -> indicator_name\n  Parent Code -> parent_code\n  Parent Name -> parent_name\n  Area Code -> area_code\n  Area Name -> area_name\n  Area Type -> area_type\n  AREA_ID -> area_id\n  Sex -> sex\n  Age -> age\n  Category Type -> category_type\n  Category -> category\n  Time period -> time_period\n  Value -> value\n  Lower CI 95.0 limit -> lower_ci_95_0_limit\n  Upper CI 95.0 limit -> upper_ci_95_0_limit\n  Lower CI 99.8 limit -> lower_ci_99_8_limit\n  Upper CI 99.8 limit -> upper_ci_99_8_limit\n  Count -> count\n  Denominator -> denominator\n  Value note -> value_note\n  Recent Trend -> recent_trend\n  Compared to England value or percentiles -> compared_to_england_value_or_percentiles\n  Compared to percentiles -> compared_to_percentiles\n  Time period Sortable -> time_period_sortable\n  New data -> new_data\n  Compared to goal -> compared_to_goal\n  Time period range -> time_period_range\n  DATE_UPDATED_LOCAL -> date_updated_local\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "Indicator ID" as indicator_id,
    "Indicator Name" as indicator_name,
    "Parent Code" as parent_code,
    "Parent Name" as parent_name,
    "Area Code" as area_code,
    "Area Name" as area_name,
    "Area Type" as area_type,
    "AREA_ID" as area_id,
    "Sex" as sex,
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
