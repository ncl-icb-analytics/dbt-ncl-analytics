{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Diagnosis \ndbt: source(''dictionary_dbo'', ''Diagnosis'') \nColumns:\n  SK_DiagnosisID -> sk_diagnosis_id\n  Code -> code\n  AltCode -> alt_code\n  Description -> description\n  ShortDescription -> short_description\n  Modifiers -> modifiers\n  Chapter_Number -> chapter_number\n  Chapter -> chapter\n  SubChapter -> sub_chapter\n  Gender_Mask -> gender_mask\n  Min_Age -> min_age\n  Max_Age -> max_age\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  SubChapterCode -> sub_chapter_code\n  SubChapter2 -> sub_chapter2\n  SubChapter2Code -> sub_chapter2_code\n  SubChapter3 -> sub_chapter3\n  SubChapter3Code -> sub_chapter3_code"
    )
}}
select
    "SK_DiagnosisID" as sk_diagnosis_id,
    "Code" as code,
    "AltCode" as alt_code,
    "Description" as description,
    "ShortDescription" as short_description,
    "Modifiers" as modifiers,
    "Chapter_Number" as chapter_number,
    "Chapter" as chapter,
    "SubChapter" as sub_chapter,
    "Gender_Mask" as gender_mask,
    "Min_Age" as min_age,
    "Max_Age" as max_age,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SubChapterCode" as sub_chapter_code,
    "SubChapter2" as sub_chapter2,
    "SubChapter2Code" as sub_chapter2_code,
    "SubChapter3" as sub_chapter3,
    "SubChapter3Code" as sub_chapter3_code
from {{ source('dictionary_dbo', 'Diagnosis') }}
