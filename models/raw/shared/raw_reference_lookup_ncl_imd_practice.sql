{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.IMD_PRACTICE \ndbt: source(''reference_lookup_ncl'', ''IMD_PRACTICE'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  DATE_INDICATOR -> date_indicator\n  IMD_DECILE -> imd_decile"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "DATE_INDICATOR" as date_indicator,
    "IMD_DECILE" as imd_decile
from {{ source('reference_lookup_ncl', 'IMD_PRACTICE') }}
