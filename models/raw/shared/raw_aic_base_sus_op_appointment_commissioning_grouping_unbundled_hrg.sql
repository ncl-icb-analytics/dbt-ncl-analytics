-- Raw layer model for aic.BASE_SUS__OP_APPOINTMENT_COMMISSIONING_GROUPING_UNBUNDLED_HRG
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MULTIPLE_APPLIES" as multiple_applies,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "TARIFF" as tariff,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "CODE" as code,
    "TARIFF_APPLIED" as tariff_applied
from {{ source('aic', 'BASE_SUS__OP_APPOINTMENT_COMMISSIONING_GROUPING_UNBUNDLED_HRG') }}
