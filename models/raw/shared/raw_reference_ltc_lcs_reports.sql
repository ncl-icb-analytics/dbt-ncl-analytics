-- Raw layer model for reference_terminology.LTC_LCS_REPORTS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "REPORT_ID" as report_id,
    "REPORT_NAME" as report_name,
    "SEARCH_NAME" as search_name,
    "FOLDER_PATH" as folder_path,
    "XML_FILE_NAME" as xml_file_name,
    "PARSED_AT" as parsed_at
from {{ source('reference_terminology', 'LTC_LCS_REPORTS') }}
