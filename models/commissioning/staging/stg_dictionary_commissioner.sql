-- Staging model for dictionary.Commissioner
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

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
from {{ source('dictionary', 'Commissioner') }}
