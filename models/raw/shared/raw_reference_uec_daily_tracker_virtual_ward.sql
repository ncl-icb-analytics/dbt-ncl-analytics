-- Raw layer model for reference_analyst_managed.UEC__DAILY__TRACKER_VIRTUAL_WARD
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE_DATA" as date_data,
    "CAPACITY" as capacity,
    "OCCUPIED" as occupied,
    "SYSTEM_VALUE" as system_value,
    "IS_INCLUDES_PAEDIATRIC" as is_includes_paediatric,
    "_TIMESTAMP" as timestamp
from {{ source('reference_analyst_managed', 'UEC__DAILY__TRACKER_VIRTUAL_WARD') }}
