-- Staging model for dictionary.OrganisationType
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationTypeID" as sk_organisationtypeid,
    "OrganisationType" as organisationtype,
    "ShortOrganisationType" as shortorganisationtype,
    "CodeAllocatedBy" as codeallocatedby,
    "IsOrganisationCode" as isorganisationcode,
    "IsLocationCode" as islocationcode,
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid
from {{ source('dictionary', 'OrganisationType') }}
