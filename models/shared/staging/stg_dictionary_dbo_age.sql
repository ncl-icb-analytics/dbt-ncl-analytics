-- Staging model for dictionary_dbo.Age
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeID" as sk_ageid,
    "Age" as age,
    "SK_AgeBandID" as sk_agebandid,
    "SK_AgeBandGPID" as sk_agebandgpid
from {{ source('dictionary_dbo', 'Age') }}
