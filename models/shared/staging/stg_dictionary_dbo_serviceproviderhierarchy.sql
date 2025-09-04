-- Staging model for dictionary_dbo.ServiceProviderHierarchy
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "SK_ServiceProviderGroupParentID" as sk_service_provider_group_parent_id,
    "Level" as level,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ServiceProviderHierarchy') }}
