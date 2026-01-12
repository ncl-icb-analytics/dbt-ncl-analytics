{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationRoleType \ndbt: source(''dictionary_dbo'', ''OrganisationRoleType'') \nColumns:\n  Code -> code\n  Name -> name"
    )
}}
select
    "Code" as code,
    "Name" as name
from {{ source('dictionary_dbo', 'OrganisationRoleType') }}
