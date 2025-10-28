-- Raw layer model for aic.STG_GP__PERSON_PATIENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PID_PT_ID" as pid_pt_id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "DQ_IS_MULTIPLE_PT" as dq_is_multiple_pt
from {{ source('aic', 'STG_GP__PERSON_PATIENT') }}
