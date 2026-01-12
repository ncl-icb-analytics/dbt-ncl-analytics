{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.F&AMapping \ndbt: source(''dictionary_dbo'', ''F&AMapping'') \nColumns:\n  SK_PODID -> sk_podid\n  SK_LevelID -> sk_level_id\n  Finance/Activity -> finance_activity\n  Description -> description"
    )
}}
select
    "SK_PODID" as sk_podid,
    "SK_LevelID" as sk_level_id,
    "Finance/Activity" as finance_activity,
    "Description" as description
from {{ source('dictionary_dbo', 'F&AMapping') }}
