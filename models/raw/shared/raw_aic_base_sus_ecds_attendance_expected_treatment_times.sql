-- Raw layer model for aic.BASE_SUS__ECDS_ATTENDANCE_EXPECTED_TREATMENT_TIMES
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXPECTED_TREATMENT_TIMES_ID" as expected_treatment_times_id,
    "TIMESTAMP" as timestamp,
    "ALLOCATED_TIMESTAMP" as allocated_timestamp,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id
from {{ source('aic', 'BASE_SUS__ECDS_ATTENDANCE_EXPECTED_TREATMENT_TIMES') }}
