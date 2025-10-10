-- Raw layer model for reference_analyst_managed.NCL_NEIGHBOURHOOD_LSOA_2021_BACKUP
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA_2021_CODE" as lsoa_2021_code,
    "LSOA_2021_NAME" as lsoa_2021_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "START_DATE" as start_date
from {{ source('reference_analyst_managed', 'NCL_NEIGHBOURHOOD_LSOA_2021_BACKUP') }}
