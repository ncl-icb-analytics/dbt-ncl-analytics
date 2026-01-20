{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ServiceProviderGroupNational \ndbt: source(''dictionary_dbo'', ''ServiceProviderGroupNational'') \nColumns:\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  ServiceProviderGroupName -> service_provider_group_name\n  ServiceProviderGroupCode -> service_provider_group_code\n  SK_ServiceProviderTypeID -> sk_service_provider_type_id\n  StartDate -> start_date\n  EndDate -> end_date\n  IsTestOrganisation -> is_test_organisation"
    )
}}
select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "ServiceProviderGroupName" as service_provider_group_name,
    "ServiceProviderGroupCode" as service_provider_group_code,
    "SK_ServiceProviderTypeID" as sk_service_provider_type_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "IsTestOrganisation" as is_test_organisation
from {{ source('dictionary_dbo', 'ServiceProviderGroupNational') }}
