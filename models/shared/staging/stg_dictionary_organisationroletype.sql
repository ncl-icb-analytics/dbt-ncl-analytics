-- Staging model for dictionary.OrganisationRoleType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary', 'OrganisationRoleType') }}
