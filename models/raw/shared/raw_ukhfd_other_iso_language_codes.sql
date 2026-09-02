{{
    config(
        description="Raw layer: ISO 639 language codes and their UKHFD history. 1:1 passthrough with cleaned column names."
    )
}}
select
    "ISO_639_2_Code" as iso_639_2_code,
    "ISO_639_1_Code" as iso_639_1_code,
    "English_Name_Of_Language" as english_name_of_language,
    "French_Name_Of_Language" as french_name_of_language,
    "German_Name_Of_Language" as german_name_of_language,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_other', 'iso_language_codes') }}
