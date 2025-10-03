-- Raw layer model for dictionary_dbo.OrganisationRoleType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "Code" as code,
    "Name" as name
from {{ source('dictionary_dbo', 'OrganisationRoleType') }}
