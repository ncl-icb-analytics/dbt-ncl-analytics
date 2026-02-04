{{
    config(
        description="Raw layer (ptl data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PTL.PTL_MONTHLY_MAPPING \ndbt: source(''ptl'', ''PTL_MONTHLY_MAPPING'') \nColumns:\n  CENSUS_DATE -> census_date\n  FINYEAR -> finyear\n  FINMONTH -> finmonth\n  END_OF_MONTH -> end_of_month"
    )
}}
select
    "CENSUS_DATE" as census_date,
    "FINYEAR" as finyear,
    "FINMONTH" as finmonth,
    "END_OF_MONTH" as end_of_month
from {{ source('ptl', 'PTL_MONTHLY_MAPPING') }}
