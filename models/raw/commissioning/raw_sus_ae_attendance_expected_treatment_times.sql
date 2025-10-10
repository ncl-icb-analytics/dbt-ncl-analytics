-- Raw layer model for sus_ae.attendance.expected_treatment_times
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXPECTED_TREATMENT_TIMES_ID" as expected_treatment_times_id,
    "timestamp" as timestamp,
    "allocated_timestamp" as allocated_timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ae', 'attendance.expected_treatment_times') }}
