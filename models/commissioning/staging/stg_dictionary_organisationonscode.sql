-- Staging model for dictionary.OrganisationONSCode
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID" as sk_organisationid,
    "ONS_Code" as ons_code
from {{ source('dictionary', 'OrganisationONSCode') }}
