-- Staging model for dictionary_dbo.AllSPGs
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "Level" as level,
    "Type" as type,
    "OriginalID" as original_id,
    "Code" as code,
    "Name" as name,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "IsTestOrganisation" as is_test_organisation,
    "IsDormant" as is_dormant,
    "IsActive" as is_active
from {{ source('dictionary_dbo', 'AllSPGs') }}
