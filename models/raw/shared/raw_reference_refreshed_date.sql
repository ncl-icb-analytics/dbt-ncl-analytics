-- Raw layer model for reference_analyst_managed.REFRESHED_DATE
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "REFRESHED_DATE" as refreshed_date,
    "PRACTICE_COUNT" as practice_count
from {{ source('reference_analyst_managed', 'REFRESHED_DATE') }}
