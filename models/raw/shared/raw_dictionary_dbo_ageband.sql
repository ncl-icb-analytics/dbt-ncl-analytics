{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.AgeBand \ndbt: source(''dictionary_dbo'', ''AgeBand'') \nColumns:\n  SK_AgeBandID -> sk_age_band_id\n  BK_AgeBand -> bk_age_band\n  AgeBandStarts -> age_band_starts\n  AgeBandEnds -> age_band_ends\n  CreatedDateTime -> created_date_time\n  LastUpdateDateTime -> last_update_date_time"
    )
}}
select
    "SK_AgeBandID" as sk_age_band_id,
    "BK_AgeBand" as bk_age_band,
    "AgeBandStarts" as age_band_starts,
    "AgeBandEnds" as age_band_ends,
    "CreatedDateTime" as created_date_time,
    "LastUpdateDateTime" as last_update_date_time
from {{ source('dictionary_dbo', 'AgeBand') }}
