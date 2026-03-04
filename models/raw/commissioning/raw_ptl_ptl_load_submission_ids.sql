{{
    config(
        description="Raw layer (ptl data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PTL.PTL_LOAD_SUBMISSION_IDS \ndbt: source(''ptl'', ''PTL_LOAD_SUBMISSION_IDS'') \nColumns:\n  CENSUS_DATE -> census_date\n  CENSUS_DATE_NUMERIC -> census_date_numeric\n  LOAD_DATE -> load_date\n  SUBMISSION_ID -> submission_id"
    )
}}
select
    "CENSUS_DATE" as census_date,
    "CENSUS_DATE_NUMERIC" as census_date_numeric,
    "LOAD_DATE" as load_date,
    "SUBMISSION_ID" as submission_id
from {{ source('ptl', 'PTL_LOAD_SUBMISSION_IDS') }}
