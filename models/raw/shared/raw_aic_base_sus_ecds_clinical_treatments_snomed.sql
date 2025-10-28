-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_TREATMENTS_SNOMED
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EQUIVALENT_AE_CODE" as equivalent_ae_code,
    "TIME" as time,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "IS_CODE_APPROVED" as is_code_approved,
    "SNOMED_ID" as snomed_id,
    "DATE" as date,
    "TIMESTAMP" as timestamp,
    "CODE" as code
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_TREATMENTS_SNOMED') }}
