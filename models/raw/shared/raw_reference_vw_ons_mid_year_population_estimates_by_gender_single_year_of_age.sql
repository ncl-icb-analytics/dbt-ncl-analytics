{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE \ndbt: source(''reference_analyst_managed'', ''VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE'') \nColumns:\n  BOROUGH -> borough\n  GENDER -> gender\n  AGE_GROUP -> age_group\n  AGE_BAND_5YEAR -> age_band_5_year\n  AGE_BAND_5YEAR_POP_HEALTH -> age_band_5_year_pop_health\n  YEAR -> year\n  ONS_POPULATION -> ons_population"
    )
}}
select
    "BOROUGH" as borough,
    "GENDER" as gender,
    "AGE_GROUP" as age_group,
    "AGE_BAND_5YEAR" as age_band_5_year,
    "AGE_BAND_5YEAR_POP_HEALTH" as age_band_5_year_pop_health,
    "YEAR" as year,
    "ONS_POPULATION" as ons_population
from {{ source('reference_analyst_managed', 'VW_ONS_MID_YEAR_POPULATION_ESTIMATES_BY_GENDER_SINGLE_YEAR_OF_AGE') }}
