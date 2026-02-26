{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_REPORTS \ndbt: source(''reference_terminology'', ''LTC_LCS_REPORTS'') \nColumns:\n  REPORT_ID -> report_id\n  REPORT_XML_ID -> report_xml_id\n  REPORT_NAME -> report_name\n  SEARCH_NAME -> search_name\n  DESCRIPTION -> description\n  PARENT_TYPE -> parent_type\n  PARENT_REPORT_ID -> parent_report_id\n  FOLDER_PATH -> folder_path\n  XML_FILE_NAME -> xml_file_name\n  EQUIVALENCE_FILTER_SETTING -> equivalence_filter_setting\n  PARSED_AT -> parsed_at"
    )
}}
select
    "REPORT_ID" as report_id,
    "REPORT_XML_ID" as report_xml_id,
    "REPORT_NAME" as report_name,
    "SEARCH_NAME" as search_name,
    "DESCRIPTION" as description,
    "PARENT_TYPE" as parent_type,
    "PARENT_REPORT_ID" as parent_report_id,
    "FOLDER_PATH" as folder_path,
    "XML_FILE_NAME" as xml_file_name,
    "EQUIVALENCE_FILTER_SETTING" as equivalence_filter_setting,
    "PARSED_AT" as parsed_at
from {{ source('reference_terminology', 'LTC_LCS_REPORTS') }}
