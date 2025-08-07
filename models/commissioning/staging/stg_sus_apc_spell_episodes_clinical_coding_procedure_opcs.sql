-- Staging model for sus_apc.spell.episodes.clinical_coding.procedure.opcs
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "dmicImportLogId" as dmicimportlogid,
    "main_operating_professional.registration_issuer" as main_operating_professional_registration_issuer,
    "date" as date,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "OPCS_ID" as opcs_id,
    "code" as code
from {{ source('sus_apc', 'spell.episodes.clinical_coding.procedure.opcs') }}
