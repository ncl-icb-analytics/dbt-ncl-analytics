-- Raw layer model for reference_lookup_ncl.NCL_GP_PRACTICE_NEIGHBOURHOOD
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code
from {{ source('reference_lookup_ncl', 'NCL_GP_PRACTICE_NEIGHBOURHOOD') }}
