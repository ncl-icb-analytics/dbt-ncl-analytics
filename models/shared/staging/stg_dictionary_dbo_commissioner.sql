-- Staging model for dictionary_dbo.Commissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CommissionerID" as sk_commissionerid,
    "CommissionerName" as commissionername,
    "CommissionerType" as commissionertype,
    "CommissionerCode" as commissionercode,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsCustomer" as iscustomer,
    "IsTestOrganisation" as istestorganisation
from {{ source('dictionary_dbo', 'Commissioner') }}
