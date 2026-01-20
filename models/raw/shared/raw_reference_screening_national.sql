{{
    config(
        description="Raw layer (Cancer screening data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__SCREENING.SCREENING_NATIONAL \ndbt: source(''reference_cancer_screening'', ''SCREENING_NATIONAL'') \nColumns:\n  Organisation Code -> organisation_code\n  Organisation Name -> organisation_name\n  Programme -> programme\n  Month of Date -> month_of_date\n  Cohort Age Range -> cohort_age_range\n  Cohort Description -> cohort_description\n  Denominator Name -> denominator_name\n  Denominator -> denominator\n  Numerator -> numerator\n  Performance -> performance\n  Acceptable -> acceptable\n  Achievable -> achievable\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "Organisation Code" as organisation_code,
    "Organisation Name" as organisation_name,
    "Programme" as programme,
    "Month of Date" as month_of_date,
    "Cohort Age Range" as cohort_age_range,
    "Cohort Description" as cohort_description,
    "Denominator Name" as denominator_name,
    "Denominator" as denominator,
    "Numerator" as numerator,
    "Performance" as performance,
    "Acceptable" as acceptable,
    "Achievable" as achievable,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_screening', 'SCREENING_NATIONAL') }}
