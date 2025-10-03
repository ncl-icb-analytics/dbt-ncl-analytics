-- Raw layer model for dictionary_dbo.HRG
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_HRGID" as sk_hrgid,
    "HRGCode" as hrg_code,
    "HRGDescription" as hrg_description,
    "HRGChapterKey" as hrg_chapter_key,
    "HRGChapter" as hrg_chapter,
    "HRGSubchapterKey" as hrg_subchapter_key,
    "HRGSubchapter" as hrg_subchapter,
    "HRG_Version" as hrg_version,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'HRG') }}
