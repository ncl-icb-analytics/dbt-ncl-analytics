{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__RCRD_NATIONAL \ndbt: source(''reference_analyst_managed'', ''CANCER__RCRD_NATIONAL'') \nColumns:\n  GEOGRAPHY_TYPE -> geography_type\n  GEOGRAPHY -> geography\n  YEAR -> year\n  MONTH -> month\n  DATE -> date\n  CANCER_GROUP -> cancer_group\n  Cancer group (broad) -> cancer_group_broad\n  Cancer group (detailed) -> cancer_group_detailed\n  METRIC -> metric\n  BREAKDOWN -> breakdown\n  DEMOGRAPHIC -> demographic\n  Completeness treatment follow-up -> completeness_treatment_follow_up\n  NUMERATOR -> numerator\n  DENOMINATOR -> denominator\n  STATISTIC -> statistic\n  Numerator (12m) -> numerator_12m\n  Denominator (12m) -> denominator_12m\n  Statistic (12m) -> statistic_12m"
    )
}}
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
