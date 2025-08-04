-- Staging model for dictionary.OrganisationRelationshipType
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary', 'OrganisationRelationshipType') }}
