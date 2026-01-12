{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.HRG \ndbt: source(''dictionary_dbo'', ''HRG'') \nColumns:\n  SK_HRGID -> sk_hrgid\n  HRGCode -> hrg_code\n  HRGDescription -> hrg_description\n  HRGChapterKey -> hrg_chapter_key\n  HRGChapter -> hrg_chapter\n  HRGSubchapterKey -> hrg_subchapter_key\n  HRGSubchapter -> hrg_subchapter\n  HRG_Version -> hrg_version\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
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
