{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.ONS_POPULATION_AGE_GENDER_LSOA_2024 \ndbt: source(''reference_analyst_managed'', ''ONS_POPULATION_AGE_GENDER_LSOA_2024'') \nColumns:\n  LAD_2023_CODE -> lad_2023_code\n  LSOA_2021_CODE -> lsoa_2021_code\n  AGE -> age\n  PERSONS -> persons\n  GENDER -> gender"
    )
}}
select
    "LAD_2023_CODE" as lad_2023_code,
    "LSOA_2021_CODE" as lsoa_2021_code,
    "AGE" as age,
    "PERSONS" as persons,
    "GENDER" as gender
from {{ source('reference_analyst_managed', 'ONS_POPULATION_AGE_GENDER_LSOA_2024') }}
