{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationFormerName \ndbt: source(''dictionary_dbo'', ''OrganisationFormerName'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  Organisation_Name -> organisation_name\n  StartDate -> start_date\n  EndDate -> end_date"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "Organisation_Name" as organisation_name,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'OrganisationFormerName') }}
