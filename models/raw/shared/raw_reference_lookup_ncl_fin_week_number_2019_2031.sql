-- Raw layer model for reference_lookup_ncl.FIN_WEEK_NUMBER_2019_2031
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "FULL_DATE" as full_date,
    "NEW_START_OF_FIN_WEEK" as new_start_of_fin_week,
    "NEW_FIN_YEAR" as new_fin_year,
    "NEW_FIN_WEEK_NUM" as new_fin_week_num
from {{ source('reference_lookup_ncl', 'FIN_WEEK_NUMBER_2019_2031') }}
