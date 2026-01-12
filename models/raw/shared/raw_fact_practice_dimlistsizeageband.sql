{{
    config(
        description="Raw layer (Practice fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PRACTICE.DimListSizeAgeBand \ndbt: source(''fact_practice'', ''DimListSizeAgeBand'') \nColumns:\n  SK_ListSizeAgeBandID -> sk_list_size_age_band_id\n  ListSizeAgeBand -> list_size_age_band\n  AgeBandStarts -> age_band_starts\n  AgeBandEnds -> age_band_ends\n  IsStandardAgeBand -> is_standard_age_band\n  IsExtendedAgeBand -> is_extended_age_band"
    )
}}
select
    "SK_ListSizeAgeBandID" as sk_list_size_age_band_id,
    "ListSizeAgeBand" as list_size_age_band,
    "AgeBandStarts" as age_band_starts,
    "AgeBandEnds" as age_band_ends,
    "IsStandardAgeBand" as is_standard_age_band,
    "IsExtendedAgeBand" as is_extended_age_band
from {{ source('fact_practice', 'DimListSizeAgeBand') }}
