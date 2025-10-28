-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_COMORBIDITIES
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "COMORBIDITIES_ID" as comorbidities_id,
    "CODE" as code,
    "IS_CODE_APPROVED" as is_code_approved,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_COMORBIDITIES') }}
