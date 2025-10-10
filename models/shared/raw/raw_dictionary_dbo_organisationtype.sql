-- Raw layer model for dictionary_dbo.OrganisationType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationTypeID" as sk_organisation_type_id,
    "OrganisationType" as organisation_type,
    "ShortOrganisationType" as short_organisation_type,
    "CodeAllocatedBy" as code_allocated_by,
    "IsOrganisationCode" as is_organisation_code,
    "IsLocationCode" as is_location_code,
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id
from {{ source('dictionary_dbo', 'OrganisationType') }}
