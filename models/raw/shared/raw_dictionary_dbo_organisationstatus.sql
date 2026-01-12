{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationStatus \ndbt: source(''dictionary_dbo'', ''OrganisationStatus'') \nColumns:\n  SK_OrganisationStatusID -> sk_organisation_status_id\n  BK_OrganisationStatus -> bk_organisation_status\n  OrganisationStatus -> organisation_status"
    )
}}
select
    "SK_OrganisationStatusID" as sk_organisation_status_id,
    "BK_OrganisationStatus" as bk_organisation_status,
    "OrganisationStatus" as organisation_status
from {{ source('dictionary_dbo', 'OrganisationStatus') }}
