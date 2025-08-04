-- Staging model for dictionary.OrganisationStatus
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationStatusID" as sk_organisationstatusid,
    "BK_OrganisationStatus" as bk_organisationstatus,
    "OrganisationStatus" as organisationstatus
from {{ source('dictionary', 'OrganisationStatus') }}
