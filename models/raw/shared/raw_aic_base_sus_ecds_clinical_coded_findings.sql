-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_CODED_FINDINGS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CODED_FINDINGS_ID" as coded_findings_id,
    "CODE" as code,
    "IS_CODE_APPROVED" as is_code_approved,
    "TIMESTAMP" as timestamp,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_CODED_FINDINGS') }}
