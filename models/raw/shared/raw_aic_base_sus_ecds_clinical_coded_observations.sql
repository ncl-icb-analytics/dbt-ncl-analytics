-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_CODED_OBSERVATIONS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "IS_CODE_APPROVED" as is_code_approved,
    "VALUE" as value,
    "CODE" as code,
    "CODED_OBSERVATIONS_ID" as coded_observations_id,
    "TIMESTAMP" as timestamp,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "UCUM_UNIT_OF_MEASUREMENT" as ucum_unit_of_measurement,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_CODED_OBSERVATIONS') }}
