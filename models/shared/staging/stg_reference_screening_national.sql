-- Staging model for reference_cancer_screening.SCREENING_NATIONAL
-- Source: "DATA_LAKE__NCL"."CANCER__SCREENING"
-- Description: Cancer screening data

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
