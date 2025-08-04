-- Staging model for dictionary.OrganisationHierarchyPractice
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Parent" as sk_organisationid_parent,
    "Level" as level
from {{ source('dictionary', 'OrganisationHierarchyPractice') }}
