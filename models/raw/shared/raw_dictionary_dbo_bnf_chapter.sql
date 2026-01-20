{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.BNF_Chapter \ndbt: source(''dictionary_dbo'', ''BNF_Chapter'') \nColumns:\n  SK_BNFChapterID -> sk_bnf_chapter_id\n  SK_BNFChapterParentID -> sk_bnf_chapter_parent_id\n  Chapter_Code -> chapter_code\n  Chapter_Code_Alt -> chapter_code_alt\n  Chapter_Code_Alt_Pad -> chapter_code_alt_pad\n  Chapter_Name -> chapter_name\n  Chapter_Path -> chapter_path\n  Chapter_Path_Depth -> chapter_path_depth\n  IsOfficialBNF -> is_official_bnf\n  URL -> url\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
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
