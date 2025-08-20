-- Staging model for dictionary_dbo.AgeBand_AgeBandGP_Lookup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeBandID" as sk_agebandid,
    "SK_AgeBandGPID" as sk_agebandgpid
from {{ source('dictionary_dbo', 'AgeBand_AgeBandGP_Lookup') }}
