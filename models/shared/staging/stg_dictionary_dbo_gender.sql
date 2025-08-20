-- Staging model for dictionary_dbo.Gender
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_GenderID" as sk_genderid,
    "Gender" as gender,
    "GenderCode" as gendercode,
    "GenderCode1" as gendercode1,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "GenderCode2" as gendercode2
from {{ source('dictionary_dbo', 'Gender') }}
