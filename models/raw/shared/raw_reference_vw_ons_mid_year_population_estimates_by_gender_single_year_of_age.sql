-- Raw layer model for reference_analyst_managed.VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "BOROUGH" as borough,
    "GENDER" as gender,
    "AGE_GROUP" as age_group,
    "YEAR" as year,
    "ONS_POPULATION" as ons_population
from {{ source('reference_analyst_managed', 'VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE') }}
