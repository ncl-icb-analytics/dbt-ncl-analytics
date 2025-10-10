-- Raw layer model for reference_analyst_managed.UEC__DAILY__TRACKER_P2_CAPACITY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE_DATA" as date_data,
    "PROVIDER" as provider,
    "UNIT" as unit,
    "BEDS_AVAILABLE" as beds_available,
    "BEDS_OCCUPIED" as beds_occupied,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_P2_CAPACITY') }}
