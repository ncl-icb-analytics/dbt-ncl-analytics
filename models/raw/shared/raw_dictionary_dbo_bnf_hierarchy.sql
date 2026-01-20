{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.BNF_Hierarchy \ndbt: source(''dictionary_dbo'', ''BNF_Hierarchy'') \nColumns:\n  SK_BNFID -> sk_bnfid\n  SK_BNFParentID -> sk_bnf_parent_id\n  TypeNum -> type_num\n  Type -> type\n  Code -> code\n  Name -> name\n  Path -> path\n  Path_Depth -> path_depth\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_BNFID" as sk_bnfid,
    "SK_BNFParentID" as sk_bnf_parent_id,
    "TypeNum" as type_num,
    "Type" as type,
    "Code" as code,
    "Name" as name,
    "Path" as path,
    "Path_Depth" as path_depth,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'BNF_Hierarchy') }}
