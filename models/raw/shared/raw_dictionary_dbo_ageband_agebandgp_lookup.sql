{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.AgeBand_AgeBandGP_Lookup \ndbt: source(''dictionary_dbo'', ''AgeBand_AgeBandGP_Lookup'') \nColumns:\n  SK_AgeBandID -> sk_age_band_id\n  SK_AgeBandGPID -> sk_age_band_gpid"
    )
}}
select
    "SK_AgeBandID" as sk_age_band_id,
    "SK_AgeBandGPID" as sk_age_band_gpid
from {{ source('dictionary_dbo', 'AgeBand_AgeBandGP_Lookup') }}
