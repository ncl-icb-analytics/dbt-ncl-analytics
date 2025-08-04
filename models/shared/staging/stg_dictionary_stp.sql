-- Staging model for dictionary.STP
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_STPID" as sk_stpid,
    "STPCode" as stpcode,
    "STPName" as stpname,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "ODSCode" as odscode,
    "SK_OrganisationID" as sk_organisationid
from {{ source('dictionary', 'STP') }}
