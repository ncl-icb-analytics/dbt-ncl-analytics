-- Staging model for dictionary_dbo.AgeBand_GP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
