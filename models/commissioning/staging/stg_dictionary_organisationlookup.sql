-- Staging model for dictionary.OrganisationLookup
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_CommissionerID" as sk_commissionerid,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "SK_Organisation_ID" as sk_organisation_id
from {{ source('dictionary', 'OrganisationLookup') }}
