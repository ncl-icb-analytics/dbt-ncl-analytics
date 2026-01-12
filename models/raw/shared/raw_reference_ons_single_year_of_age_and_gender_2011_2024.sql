{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.ONS_SINGLE_YEAR_OF_AGE_AND_GENDER_2011_2024 \ndbt: source(''reference_analyst_managed'', ''ONS_SINGLE_YEAR_OF_AGE_AND_GENDER_2011_2024'') \nColumns:\n  LADCODE23 -> ladcode23\n  LANAME23 -> laname23\n  COUNTRY -> country\n  SEX -> sex\n  AGE -> age\n  POPULATION_2011 -> population_2011\n  POPULATION_2012 -> population_2012\n  POPULATION_2013 -> population_2013\n  POPULATION_2014 -> population_2014\n  POPULATION_2015 -> population_2015\n  POPULATION_2016 -> population_2016\n  POPULATION_2017 -> population_2017\n  POPULATION_2018 -> population_2018\n  POPULATION_2019 -> population_2019\n  POPULATION_2020 -> population_2020\n  POPULATION_2021 -> population_2021\n  POPULATION_2022 -> population_2022\n  POPULATION_2023 -> population_2023\n  POPULATION_2024 -> population_2024"
    )
}}
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
