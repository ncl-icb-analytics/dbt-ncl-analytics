-- Staging model for dictionary_dbo.ServiceProviderGroup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "ServiceProviderGroupName" as service_provider_group_name,
    "ServiceProviderGroupCode" as service_provider_group_code,
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "IsTestOrganisation" as is_test_organisation
from {{ source('dictionary_dbo', 'ServiceProviderGroup') }}
