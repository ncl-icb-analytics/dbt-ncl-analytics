-- Staging model for dictionary.ServiceProviderGroupNational
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "ServiceProviderGroupName" as serviceprovidergroupname,
    "ServiceProviderGroupCode" as serviceprovidergroupcode,
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "IsTestOrganisation" as istestorganisation
from {{ source('dictionary', 'ServiceProviderGroupNational') }}
