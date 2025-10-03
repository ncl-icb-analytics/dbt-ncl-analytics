-- Raw layer model for sus_apc.spell.episodes.clinical_coding.procedure.snomed
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "SNOMED_ID" as snomed_id,
    "code" as code,
    "sequence_number" as sequence_number,
    "timestamp" as timestamp,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "main_operating_professional.registration_issuer" as main_operating_professional_registration_issuer,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.procedure.snomed') }}
