-- Raw layer model for dictionary_dbo.ServiceProviderGroupNational
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "ServiceProviderGroupName" as service_provider_group_name,
    "ServiceProviderGroupCode" as service_provider_group_code,
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "IsTestOrganisation" as is_test_organisation
from {{ source('dictionary_dbo', 'ServiceProviderGroupNational') }}
