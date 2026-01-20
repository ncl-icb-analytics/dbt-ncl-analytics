{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.STP \ndbt: source(''dictionary_dbo'', ''STP'') \nColumns:\n  SK_STPID -> sk_stpid\n  STPCode -> stp_code\n  STPName -> stp_name\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  ODSCode -> ods_code\n  SK_OrganisationID -> sk_organisation_id"
    )
}}
select
    "SK_STPID" as sk_stpid,
    "STPCode" as stp_code,
    "STPName" as stp_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "ODSCode" as ods_code,
    "SK_OrganisationID" as sk_organisation_id
from {{ source('dictionary_dbo', 'STP') }}
