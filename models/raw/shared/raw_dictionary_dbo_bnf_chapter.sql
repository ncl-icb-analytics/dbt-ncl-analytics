-- Raw layer model for dictionary_dbo.BNF_Chapter
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_BNFChapterID" as sk_bnf_chapter_id,
    "SK_BNFChapterParentID" as sk_bnf_chapter_parent_id,
    "Chapter_Code" as chapter_code,
    "Chapter_Code_Alt" as chapter_code_alt,
    "Chapter_Code_Alt_Pad" as chapter_code_alt_pad,
    "Chapter_Name" as chapter_name,
    "Chapter_Path" as chapter_path,
    "Chapter_Path_Depth" as chapter_path_depth,
    "IsOfficialBNF" as is_official_bnf,
    "URL" as url,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'BNF_Chapter') }}
