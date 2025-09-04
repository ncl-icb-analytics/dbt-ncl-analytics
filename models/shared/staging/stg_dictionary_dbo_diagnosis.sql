-- Staging model for dictionary_dbo.Diagnosis
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
