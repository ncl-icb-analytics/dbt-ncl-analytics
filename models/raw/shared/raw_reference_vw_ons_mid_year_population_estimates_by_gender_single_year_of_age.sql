{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE \ndbt: source(''reference_analyst_managed'', ''VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE'') \nColumns:\n  BOROUGH -> borough\n  GENDER -> gender\n  AGE_GROUP -> age_group\n  YEAR -> year\n  ONS_POPULATION -> ons_population"
    )
}}
select
    "BOROUGH" as borough,
    "GENDER" as gender,
    "AGE_GROUP" as age_group,
    "YEAR" as year,
    "ONS_POPULATION" as ons_population
from {{ source('reference_analyst_managed', 'VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE') }}
