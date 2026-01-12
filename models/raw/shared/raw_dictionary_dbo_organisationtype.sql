{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationType \ndbt: source(''dictionary_dbo'', ''OrganisationType'') \nColumns:\n  SK_OrganisationTypeID -> sk_organisation_type_id\n  OrganisationType -> organisation_type\n  ShortOrganisationType -> short_organisation_type\n  CodeAllocatedBy -> code_allocated_by\n  IsOrganisationCode -> is_organisation_code\n  IsLocationCode -> is_location_code\n  SK_ServiceProviderTypeID -> sk_service_provider_type_id"
    )
}}
select
    "SK_OrganisationTypeID" as sk_organisation_type_id,
    "OrganisationType" as organisation_type,
    "ShortOrganisationType" as short_organisation_type,
    "CodeAllocatedBy" as code_allocated_by,
    "IsOrganisationCode" as is_organisation_code,
    "IsLocationCode" as is_location_code,
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id
from {{ source('dictionary_dbo', 'OrganisationType') }}
