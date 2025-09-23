-- Staging model for reference_data_management.GP_WEIGHTED_LIST_SIZE
-- Source: "DATA_LAKE__NCL"."DATA_MANAGEMENT"
-- Description: Data management reference datasets

select
    "SITE" as site,
    "FINANCIAL_QUARTER_DATE" as financial_quarter_date,
    "PCT" as pct,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "GMS_PMS_FLAG" as gms_pms_flag,
    "COMMISSIONER_CODE" as commissioner_code,
    "COMMISSIONER_NAME" as commissioner_name,
    "PRACTICE_LIST_SIZE" as practice_list_size,
    "PRACTICE_NORMALISED_WEIGHTED_LIST_SIZE" as practice_normalised_weighted_list_size,
    "REPORT_EXECUTION_DATETIME" as report_execution_datetime
from {{ source('reference_data_management', 'GP_WEIGHTED_LIST_SIZE') }}
