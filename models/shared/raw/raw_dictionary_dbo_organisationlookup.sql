-- Raw layer model for dictionary_dbo.OrganisationLookup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID" as sk_organisation_id,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_Organisation_ID" as sk_organisation_id_1
from {{ source('dictionary_dbo', 'OrganisationLookup') }}
