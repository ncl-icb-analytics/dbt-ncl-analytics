-- Staging model for dictionary.OrganisationFormerName
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID" as sk_organisationid,
    "Organisation_Name" as organisation_name,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary', 'OrganisationFormerName') }}
