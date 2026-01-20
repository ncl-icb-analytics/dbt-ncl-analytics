{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Age \ndbt: source(''dictionary_dbo'', ''Age'') \nColumns:\n  SK_AgeID -> sk_age_id\n  Age -> age\n  SK_AgeBandID -> sk_age_band_id\n  SK_AgeBandGPID -> sk_age_band_gpid"
    )
}}
select
    "SK_AgeID" as sk_age_id,
    "Age" as age,
    "SK_AgeBandID" as sk_age_band_id,
    "SK_AgeBandGPID" as sk_age_band_gpid
from {{ source('dictionary_dbo', 'Age') }}
