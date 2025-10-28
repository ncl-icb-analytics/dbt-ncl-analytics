-- Raw layer model for aic.BASE_SUS__APC_SPELL_EPISODES_COMMISSIONING_GROUPING_UNBUNDLED_HRG
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "TARIFF" as tariff,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "CODE" as code,
    "ADULT_CC_TARIFF_DAYS" as adult_cc_tariff_days,
    "MULTIPLE_APPLIES" as multiple_applies
from {{ source('aic', 'BASE_SUS__APC_SPELL_EPISODES_COMMISSIONING_GROUPING_UNBUNDLED_HRG') }}
