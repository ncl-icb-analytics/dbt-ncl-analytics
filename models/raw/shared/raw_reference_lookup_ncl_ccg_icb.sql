{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.CCG_ICB \ndbt: source(''reference_lookup_ncl'', ''CCG_ICB'') \nColumns:\n  OLD_CCG_CODE -> old_ccg_code\n  NEW_CCG_CODE -> new_ccg_code\n  ICB_CODE -> icb_code\n  ICB_NAME -> icb_name"
    )
}}
select
    "OLD_CCG_CODE" as old_ccg_code,
    "NEW_CCG_CODE" as new_ccg_code,
    "ICB_CODE" as icb_code,
    "ICB_NAME" as icb_name
from {{ source('reference_lookup_ncl', 'CCG_ICB') }}
