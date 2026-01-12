{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.WORKING_AND_CALENDAR_DAYS \ndbt: source(''reference_lookup_ncl'', ''WORKING_AND_CALENDAR_DAYS'') \nColumns:\n  FIN_YEAR -> fin_year\n  FIN_MONTH -> fin_month\n  FIN_MONTH_NO -> fin_month_no\n  MONTH -> month\n  CALENDAR_DAYS -> calendar_days\n  WORKING_DAYS -> working_days\n  FIN_YEAR_SHORT -> fin_year_short\n  FIN_YEAR_SHORT_DASH -> fin_year_short_dash"
    )
}}
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
