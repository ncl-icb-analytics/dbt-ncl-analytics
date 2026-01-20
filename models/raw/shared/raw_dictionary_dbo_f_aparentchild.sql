{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.F&AParentChild \ndbt: source(''dictionary_dbo'', ''F&AParentChild'') \nColumns:\n  SK_PARENT_PODID -> sk_parent_podid\n  SK_CHILD_PODID -> sk_child_podid"
    )
}}
select
    "SK_PARENT_PODID" as sk_parent_podid,
    "SK_CHILD_PODID" as sk_child_podid
from {{ source('dictionary_dbo', 'F&AParentChild') }}
