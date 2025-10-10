-- Raw layer model for dictionary_dbo.BNF_Hierarchy
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
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
