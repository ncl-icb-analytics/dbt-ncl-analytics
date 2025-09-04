-- Staging model for dictionary_dbo.Commissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
