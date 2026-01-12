{{
    config(
        description="Raw layer (Practice fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PRACTICE.DimListSizeAge \ndbt: source(''fact_practice'', ''DimListSizeAge'') \nColumns:\n  SK_ListSizeAgeID -> sk_list_size_age_id\n  ListSizeAge -> list_size_age\n  AgeStarts -> age_starts\n  AgeEnds -> age_ends"
    )
}}
select
    "SK_ListSizeAgeID" as sk_list_size_age_id,
    "ListSizeAge" as list_size_age,
    "AgeStarts" as age_starts,
    "AgeEnds" as age_ends
from {{ source('fact_practice', 'DimListSizeAge') }}
