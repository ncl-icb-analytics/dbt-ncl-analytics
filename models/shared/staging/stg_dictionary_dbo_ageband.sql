-- Staging model for dictionary_dbo.AgeBand
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeBandID" as sk_age_band_id,
    "BK_AgeBand" as bk_age_band,
    "AgeBandStarts" as age_band_starts,
    "AgeBandEnds" as age_band_ends,
    "CreatedDateTime" as created_date_time,
    "LastUpdateDateTime" as last_update_date_time
from {{ source('dictionary_dbo', 'AgeBand') }}
