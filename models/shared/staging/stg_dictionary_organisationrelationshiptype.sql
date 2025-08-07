-- Staging model for dictionary.OrganisationRelationshipType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary', 'OrganisationRelationshipType') }}
