-- Raw layer model for reference_analyst_managed.ONS_SINGLE_YEAR_OF_AGE_AND_GENDER_2011_2024
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "LADCODE23" as ladcode23,
    "LANAME23" as laname23,
    "COUNTRY" as country,
    "SEX" as sex,
    "AGE" as age,
    "POPULATION_2011" as population_2011,
    "POPULATION_2012" as population_2012,
    "POPULATION_2013" as population_2013,
    "POPULATION_2014" as population_2014,
    "POPULATION_2015" as population_2015,
    "POPULATION_2016" as population_2016,
    "POPULATION_2017" as population_2017,
    "POPULATION_2018" as population_2018,
    "POPULATION_2019" as population_2019,
    "POPULATION_2020" as population_2020,
    "POPULATION_2021" as population_2021,
    "POPULATION_2022" as population_2022,
    "POPULATION_2023" as population_2023,
    "POPULATION_2024" as population_2024
from {{ source('reference_analyst_managed', 'ONS_SINGLE_YEAR_OF_AGE_AND_GENDER_2011_2024') }}
