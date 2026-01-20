{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.F&ALevel \ndbt: source(''dictionary_dbo'', ''F&ALevel'') \nColumns:\n  SK_LevelID -> sk_level_id\n  LevelName -> level_name"
    )
}}
select
    "SK_LevelID" as sk_level_id,
    "LevelName" as level_name
from {{ source('dictionary_dbo', 'F&ALevel') }}
