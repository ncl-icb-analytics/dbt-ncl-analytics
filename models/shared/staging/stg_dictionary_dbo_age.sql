-- Staging model for dictionary_dbo.Age
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeID" as sk_age_id,
    "Age" as age,
    "SK_AgeBandID" as sk_age_band_id,
    "SK_AgeBandGPID" as sk_age_band_gpid
from {{ source('dictionary_dbo', 'Age') }}
