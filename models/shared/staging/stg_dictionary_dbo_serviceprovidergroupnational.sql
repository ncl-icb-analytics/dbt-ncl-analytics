-- Staging model for dictionary_dbo.ServiceProviderGroupNational
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "ServiceProviderGroupName" as serviceprovidergroupname,
    "ServiceProviderGroupCode" as serviceprovidergroupcode,
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "IsTestOrganisation" as istestorganisation
from {{ source('dictionary_dbo', 'ServiceProviderGroupNational') }}
