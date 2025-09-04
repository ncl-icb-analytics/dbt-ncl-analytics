-- Staging model for dictionary_dbo.ServiceProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ServiceProviderID" as sk_service_provider_id,
    "ServiceProviderCode" as service_provider_code,
    "ServiceProviderName" as service_provider_name,
    "ServiceProviderType" as service_provider_type,
    "SK_PostcodeID" as sk_postcode_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "IsActive" as is_active,
    "IsMainSite" as is_main_site,
    "IsTestOrganisation" as is_test_organisation,
    "IsDormant" as is_dormant,
    "ServiceProviderFullCode" as service_provider_full_code
from {{ source('dictionary_dbo', 'ServiceProvider') }}
