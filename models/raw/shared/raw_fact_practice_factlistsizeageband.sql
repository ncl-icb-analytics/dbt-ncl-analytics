{{
    config(
        description="Raw layer (Practice fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PRACTICE.FactListSizeAgeBand \ndbt: source(''fact_practice'', ''FactListSizeAgeBand'') \nColumns:\n  SK_ListSizeSourceID -> sk_list_size_source_id\n  SK_OrganisationID -> sk_organisation_id\n  SK_ListSizeAgeBandID -> sk_list_size_age_band_id\n  SK_GenderID -> sk_gender_id\n  Period -> period\n  Value -> value"
    )
}}
select
    "SK_ListSizeSourceID" as sk_list_size_source_id,
    "SK_OrganisationID" as sk_organisation_id,
    "SK_ListSizeAgeBandID" as sk_list_size_age_band_id,
    "SK_GenderID" as sk_gender_id,
    "Period" as period,
    "Value" as value
from {{ source('fact_practice', 'FactListSizeAgeBand') }}
