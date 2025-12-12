-- Raw layer model for reference_analyst_managed.CANCER__RCRD_NATIONAL
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "GEOGRAPHY_TYPE" as geography_type,
    "GEOGRAPHY" as geography,
    "YEAR" as year,
    "MONTH" as month,
    "DATE" as date,
    "CANCER_GROUP" as cancer_group,
    "Cancer group (broad)" as cancer_group_broad,
    "Cancer group (detailed)" as cancer_group_detailed,
    "METRIC" as metric,
    "BREAKDOWN" as breakdown,
    "DEMOGRAPHIC" as demographic,
    "Completeness treatment follow-up" as completeness_treatment_follow_up,
    "NUMERATOR" as numerator,
    "DENOMINATOR" as denominator,
    "STATISTIC" as statistic,
    "Numerator (12m)" as numerator_12m,
    "Denominator (12m)" as denominator_12m,
    "Statistic (12m)" as statistic_12m
from {{ source('reference_analyst_managed', 'CANCER__RCRD_NATIONAL') }}
