-- Staging model for dictionary.AgeBand_AgeBandGP_Lookup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeBandID" as sk_agebandid,
    "SK_AgeBandGPID" as sk_agebandgpid
from {{ source('dictionary', 'AgeBand_AgeBandGP_Lookup') }}
