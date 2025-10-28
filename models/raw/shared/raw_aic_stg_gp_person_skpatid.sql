-- Raw layer model for aic.STG_GP__PERSON_SKPATID
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PID_SKID_ID" as pid_skid_id,
    "PERSON_ID" as person_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "DQ_IS_MULTIPLE_PID" as dq_is_multiple_pid,
    "DQ_IS_MULTIPLE_SKID" as dq_is_multiple_skid
from {{ source('aic', 'STG_GP__PERSON_SKPATID') }}
