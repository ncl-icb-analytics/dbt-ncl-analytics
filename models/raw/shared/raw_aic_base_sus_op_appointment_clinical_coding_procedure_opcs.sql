-- Raw layer model for aic.BASE_SUS__OP_APPOINTMENT_CLINICAL_CODING_PROCEDURE_OPCS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "OPCS_ID" as opcs_id,
    "CODE" as code,
    "MAIN_OPERATING_PROFESSIONAL_IDENTIFIER" as main_operating_professional_identifier,
    "MAIN_OPERATING_PROFESSIONAL_REGISTRATION_ISSUER" as main_operating_professional_registration_issuer,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "RESPONSIBLE_ANAESTHETIST_REGISTRATION_ISSUER" as responsible_anaesthetist_registration_issuer,
    "RESPONSIBLE_ANAESTHETIST_IDENTIFIER" as responsible_anaesthetist_identifier,
    "DATE" as date
from {{ source('aic', 'BASE_SUS__OP_APPOINTMENT_CLINICAL_CODING_PROCEDURE_OPCS') }}
