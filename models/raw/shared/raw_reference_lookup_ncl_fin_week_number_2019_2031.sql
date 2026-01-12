{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.FIN_WEEK_NUMBER_2019_2031 \ndbt: source(''reference_lookup_ncl'', ''FIN_WEEK_NUMBER_2019_2031'') \nColumns:\n  FULL_DATE -> full_date\n  NEW_START_OF_FIN_WEEK -> new_start_of_fin_week\n  NEW_FIN_YEAR -> new_fin_year\n  NEW_FIN_WEEK_NUM -> new_fin_week_num"
    )
}}
select
    "FULL_DATE" as full_date,
    "NEW_START_OF_FIN_WEEK" as new_start_of_fin_week,
    "NEW_FIN_YEAR" as new_fin_year,
    "NEW_FIN_WEEK_NUM" as new_fin_week_num
from {{ source('reference_lookup_ncl', 'FIN_WEEK_NUMBER_2019_2031') }}
