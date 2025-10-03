-- Raw layer model for dictionary_dbo.PartialPostcode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PartialPostcode" as sk_partial_postcode,
    "Postcode" as postcode,
    "Longitude" as longitude,
    "Latitude" as latitude,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'PartialPostcode') }}
