-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_INVESTIGATIONS_SNOMED
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "IS_CODE_APPROVED" as is_code_approved,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "SNOMED_ID" as snomed_id,
    "CODE" as code,
    "EQUIVALENT_AE_CODE" as equivalent_ae_code,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "DATE" as date,
    "TIME" as time,
    "TIMESTAMP" as timestamp
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_INVESTIGATIONS_SNOMED') }}
