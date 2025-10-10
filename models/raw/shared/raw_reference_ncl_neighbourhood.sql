-- Raw layer model for reference_analyst_managed.NCL_NEIGHBOURHOOD
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name
from {{ source('reference_analyst_managed', 'NCL_NEIGHBOURHOOD') }}
