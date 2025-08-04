-- Staging model for dictionary.Gender
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_GenderID" as sk_genderid,
    "Gender" as gender,
    "GenderCode" as gendercode,
    "GenderCode1" as gendercode1,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "GenderCode2" as gendercode2
from {{ source('dictionary', 'Gender') }}
