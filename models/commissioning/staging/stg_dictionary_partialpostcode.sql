-- Staging model for dictionary.PartialPostcode
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PartialPostcode" as sk_partialpostcode,
    "Postcode" as postcode,
    "Longitude" as longitude,
    "Latitude" as latitude,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'PartialPostcode') }}
