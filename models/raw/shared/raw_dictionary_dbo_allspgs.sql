{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.AllSPGs \ndbt: source(''dictionary_dbo'', ''AllSPGs'') \nColumns:\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  Level -> level\n  Type -> type\n  OriginalID -> original_id\n  Code -> code\n  Name -> name\n  StartDate -> start_date\n  EndDate -> end_date\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  IsTestOrganisation -> is_test_organisation\n  IsDormant -> is_dormant\n  IsActive -> is_active"
    )
}}
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
