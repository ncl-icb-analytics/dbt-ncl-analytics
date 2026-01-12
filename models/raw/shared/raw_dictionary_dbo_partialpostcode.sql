{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PartialPostcode \ndbt: source(''dictionary_dbo'', ''PartialPostcode'') \nColumns:\n  SK_PartialPostcode -> sk_partial_postcode\n  Postcode -> postcode\n  Longitude -> longitude\n  Latitude -> latitude\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_PartialPostcode" as sk_partial_postcode,
    "Postcode" as postcode,
    "Longitude" as longitude,
    "Latitude" as latitude,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'PartialPostcode') }}
