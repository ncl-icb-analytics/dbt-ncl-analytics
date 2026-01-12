{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ServiceProvider \ndbt: source(''dictionary_dbo'', ''ServiceProvider'') \nColumns:\n  SK_ServiceProviderID -> sk_service_provider_id\n  ServiceProviderCode -> service_provider_code\n  ServiceProviderName -> service_provider_name\n  ServiceProviderType -> service_provider_type\n  SK_PostcodeID -> sk_postcode_id\n  StartDate -> start_date\n  EndDate -> end_date\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  IsActive -> is_active\n  IsMainSite -> is_main_site\n  IsTestOrganisation -> is_test_organisation\n  IsDormant -> is_dormant\n  ServiceProviderFullCode -> service_provider_full_code"
    )
}}
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
