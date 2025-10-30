-- Raw layer model for reference_lookup_ncl.IMD_PRACTICE
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRACTICE_CODE" as practice_code,
    "DATE_INDICATOR" as date_indicator,
    "IMD_DECILE" as imd_decile
from {{ source('reference_lookup_ncl', 'IMD_PRACTICE') }}
