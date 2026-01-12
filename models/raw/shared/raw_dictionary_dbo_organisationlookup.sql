{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationLookup \ndbt: source(''dictionary_dbo'', ''OrganisationLookup'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  SK_CommissionerID -> sk_commissioner_id\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  SK_Organisation_ID -> sk_organisation_id_1"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_Organisation_ID" as sk_organisation_id_1
from {{ source('dictionary_dbo', 'OrganisationLookup') }}
