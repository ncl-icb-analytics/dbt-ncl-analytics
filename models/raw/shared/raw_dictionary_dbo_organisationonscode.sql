{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationONSCode \ndbt: source(''dictionary_dbo'', ''OrganisationONSCode'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  ONS_Code -> ons_code"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "ONS_Code" as ons_code
from {{ source('dictionary_dbo', 'OrganisationONSCode') }}
