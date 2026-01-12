{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Commissioner \ndbt: source(''dictionary_dbo'', ''Commissioner'') \nColumns:\n  SK_CommissionerID -> sk_commissioner_id\n  CommissionerName -> commissioner_name\n  CommissionerType -> commissioner_type\n  CommissionerCode -> commissioner_code\n  StartDate -> start_date\n  EndDate -> end_date\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  SK_ServiceProviderGroupID -> sk_service_provider_group_id\n  IsCustomer -> is_customer\n  IsTestOrganisation -> is_test_organisation"
    )
}}
select
    "SK_CommissionerID" as sk_commissioner_id,
    "CommissionerName" as commissioner_name,
    "CommissionerType" as commissioner_type,
    "CommissionerCode" as commissioner_code,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "IsCustomer" as is_customer,
    "IsTestOrganisation" as is_test_organisation
from {{ source('dictionary_dbo', 'Commissioner') }}
