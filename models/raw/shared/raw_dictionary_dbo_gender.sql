-- Raw layer model for dictionary_dbo.Gender
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_GenderID" as sk_gender_id,
    "Gender" as gender,
    "GenderCode" as gender_code,
    "GenderCode1" as gender_code1,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "GenderCode2" as gender_code2
from {{ source('dictionary_dbo', 'Gender') }}
