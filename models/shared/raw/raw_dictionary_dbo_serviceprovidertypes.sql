-- Raw layer model for dictionary_dbo.ServiceProviderTypes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id,
    "ServiceProviderTypeDescription" as service_provider_type_description
from {{ source('dictionary_dbo', 'ServiceProviderTypes') }}
