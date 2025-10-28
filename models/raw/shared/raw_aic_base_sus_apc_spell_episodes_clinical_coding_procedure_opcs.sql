-- Raw layer model for aic.BASE_SUS__APC_SPELL_EPISODES_CLINICAL_CODING_PROCEDURE_OPCS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "MAIN_OPERATING_PROFESSIONAL_REGISTRATION_ISSUER" as main_operating_professional_registration_issuer,
    "DATE" as date,
    "MAIN_OPERATING_PROFESSIONAL_IDENTIFIER" as main_operating_professional_identifier,
    "RESPONSIBLE_ANAESTHETIST_IDENTIFIER" as responsible_anaesthetist_identifier,
    "RESPONSIBLE_ANAESTHETIST_REGISTRATION_ISSUER" as responsible_anaesthetist_registration_issuer,
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "OPCS_ID" as opcs_id,
    "CODE" as code
from {{ source('aic', 'BASE_SUS__APC_SPELL_EPISODES_CLINICAL_CODING_PROCEDURE_OPCS') }}
