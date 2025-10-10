-- Raw layer model for reference_analyst_managed.NCL_GP_PRACTICE_NEIGHBOURHOOD_BACKUP
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code
from {{ source('reference_analyst_managed', 'NCL_GP_PRACTICE_NEIGHBOURHOOD_BACKUP') }}
