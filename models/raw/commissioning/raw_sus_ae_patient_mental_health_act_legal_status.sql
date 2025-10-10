-- Raw layer model for sus_ae.patient.mental_health_act_legal_status
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "start_time" as start_time,
    "expiry_date" as expiry_date,
    "expiry_time" as expiry_time,
    "dmicImportLogId" as dmic_import_log_id,
    "assignment_timestamp" as assignment_timestamp,
    "expiry_timestamp" as expiry_timestamp,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "MENTAL_HEALTH_ACT_LEGAL_STATUS_ID" as mental_health_act_legal_status_id,
    "classification" as classification,
    "start_date" as start_date
from {{ source('sus_ae', 'patient.mental_health_act_legal_status') }}
