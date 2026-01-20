{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.AgeBand_GP \ndbt: source(''dictionary_dbo'', ''AgeBand_GP'') \nColumns:\n  SK_AgeBandGPID -> sk_age_band_gpid\n  BK_AgeBandGP -> bk_age_band_gp\n  AgeBandStarts -> age_band_starts\n  AgeBandEnds -> age_band_ends\n  CreatedDateTime -> created_date_time\n  LastUpdateDateTime -> last_update_date_time\n  SK_AgeBandID -> sk_age_band_id\n  BK_AgeBand -> bk_age_band"
    )
}}
select
    "SK_AgeBandGPID" as sk_age_band_gpid,
    "BK_AgeBandGP" as bk_age_band_gp,
    "AgeBandStarts" as age_band_starts,
    "AgeBandEnds" as age_band_ends,
    "CreatedDateTime" as created_date_time,
    "LastUpdateDateTime" as last_update_date_time,
    "SK_AgeBandID" as sk_age_band_id,
    "BK_AgeBand" as bk_age_band
from {{ source('dictionary_dbo', 'AgeBand_GP') }}
