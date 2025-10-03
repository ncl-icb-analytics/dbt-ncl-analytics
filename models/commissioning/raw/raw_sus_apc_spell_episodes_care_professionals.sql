-- Raw layer model for sus_apc.spell.episodes.care_professionals
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "professional_registration_identifier" as professional_registration_identifier,
    "main_specialty" as main_specialty,
    "treatment_function" as treatment_function,
    "local_sub_specialty" as local_sub_specialty,
    "clinical_responsibility_indicator" as clinical_responsibility_indicator,
    "dmicImportLogId" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CARE_PROFESSIONALS_ID" as care_professionals_id,
    "professional_registration_issuer" as professional_registration_issuer
from {{ source('sus_apc', 'spell.episodes.care_professionals') }}
