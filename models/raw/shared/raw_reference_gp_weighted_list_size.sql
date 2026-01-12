{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.GP_WEIGHTED_LIST_SIZE \ndbt: source(''reference_data_management'', ''GP_WEIGHTED_LIST_SIZE'') \nColumns:\n  SITE -> site\n  FINANCIAL_QUARTER_DATE -> financial_quarter_date\n  PCT -> pct\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  GMS_PMS_FLAG -> gms_pms_flag\n  COMMISSIONER_CODE -> commissioner_code\n  COMMISSIONER_NAME -> commissioner_name\n  PRACTICE_LIST_SIZE -> practice_list_size\n  PRACTICE_NORMALISED_WEIGHTED_LIST_SIZE -> practice_normalised_weighted_list_size\n  REPORT_EXECUTION_DATETIME -> report_execution_datetime"
    )
}}
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
