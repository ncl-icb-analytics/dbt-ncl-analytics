-- Staging model for dictionary_dbo.OrganisationRelationshipType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary_dbo', 'OrganisationRelationshipType') }}
