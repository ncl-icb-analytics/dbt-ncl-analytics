{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ServiceProviderHierarchy \ndbt: source(''dictionary_dbo'', ''ServiceProviderHierarchy'') \nColumns:\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  SK_ServiceProviderGroupParentID -> sk_service_provider_group_parent_id\n  Level -> level\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_ServiceProviderGroupParentID" as sk_service_provider_group_parent_id,
    "Level" as level,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ServiceProviderHierarchy') }}
