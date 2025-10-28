-- Raw layer model for aic.BASE_SUS__OP_APPOINTMENT_CLINICAL_CODING_DIAGNOSIS_ICD
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "CODE" as code,
    "PRESENT_ON_ADMISSION" as present_on_admission,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ICD_ID" as icd_id
from {{ source('aic', 'BASE_SUS__OP_APPOINTMENT_CLINICAL_CODING_DIAGNOSIS_ICD') }}
