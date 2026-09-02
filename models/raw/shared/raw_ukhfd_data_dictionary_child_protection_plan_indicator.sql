{{
    config(
        description="Raw layer: Child protection plan indicator definitions and their UKHFD history. 1:1 passthrough with cleaned column names."
    )
}}
select
    "Attr_Name" as attr_name,
    "Valid_From" as valid_from,
    "Valid_To" as valid_to,
    "Main_Code_Text" as main_code_text,
    "Sub_Code1_Text" as sub_code1_text,
    "Sub_Code2_Text" as sub_code2_text,
    "Sub_Code3_Text" as sub_code3_text,
    "Major_Category" as major_category,
    "Category" as category,
    "Main_Description" as main_description,
    "Main_Description_60_Chars" as main_description_60_chars,
    "Sub1_Description" as sub1_description,
    "Sub2_Description" as sub2_description,
    "Sub3_Description" as sub3_description,
    "Notes" as notes,
    "In_Source_Table" as in_source_table,
    "Unique_Column" as unique_column,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_data_dictionary', 'child_protection_plan_indicator') }}
