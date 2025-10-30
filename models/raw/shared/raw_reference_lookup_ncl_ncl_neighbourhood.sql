-- Raw layer model for reference_lookup_ncl.NCL_NEIGHBOURHOOD
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name
from {{ source('reference_lookup_ncl', 'NCL_NEIGHBOURHOOD') }}
