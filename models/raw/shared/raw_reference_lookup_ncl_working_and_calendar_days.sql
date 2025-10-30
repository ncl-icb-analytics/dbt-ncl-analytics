-- Raw layer model for reference_lookup_ncl.WORKING_AND_CALENDAR_DAYS
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "FIN_YEAR" as fin_year,
    "FIN_MONTH" as fin_month,
    "FIN_MONTH_NO" as fin_month_no,
    "MONTH" as month,
    "CALENDAR_DAYS" as calendar_days,
    "WORKING_DAYS" as working_days,
    "FIN_YEAR_SHORT" as fin_year_short,
    "FIN_YEAR_SHORT_DASH" as fin_year_short_dash
from {{ source('reference_lookup_ncl', 'WORKING_AND_CALENDAR_DAYS') }}
