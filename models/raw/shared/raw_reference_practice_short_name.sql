{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.PRACTICE_SHORT_NAME \ndbt: source(''reference_data_management'', ''PRACTICE_SHORT_NAME'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_SHORT_NAME -> practice_short_name"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_SHORT_NAME" as practice_short_name
from {{ source('reference_data_management', 'PRACTICE_SHORT_NAME') }}
