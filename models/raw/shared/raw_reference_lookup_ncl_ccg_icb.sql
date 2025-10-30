-- Raw layer model for reference_lookup_ncl.CCG_ICB
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "OLD_CCG_CODE" as old_ccg_code,
    "NEW_CCG_CODE" as new_ccg_code,
    "ICB_CODE" as icb_code,
    "ICB_NAME" as icb_name
from {{ source('reference_lookup_ncl', 'CCG_ICB') }}
