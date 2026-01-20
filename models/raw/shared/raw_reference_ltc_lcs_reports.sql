{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_REPORTS \ndbt: source(''reference_terminology'', ''LTC_LCS_REPORTS'') \nColumns:\n  REPORT_ID -> report_id\n  REPORT_NAME -> report_name\n  SEARCH_NAME -> search_name\n  FOLDER_PATH -> folder_path\n  XML_FILE_NAME -> xml_file_name\n  PARSED_AT -> parsed_at"
    )
}}
select
    "REPORT_ID" as report_id,
    "REPORT_NAME" as report_name,
    "SEARCH_NAME" as search_name,
    "FOLDER_PATH" as folder_path,
    "XML_FILE_NAME" as xml_file_name,
    "PARSED_AT" as parsed_at
from {{ source('reference_terminology', 'LTC_LCS_REPORTS') }}
