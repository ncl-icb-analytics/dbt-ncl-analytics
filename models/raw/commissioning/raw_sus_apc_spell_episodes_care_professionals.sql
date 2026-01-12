{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.care_professionals \ndbt: source(''sus_apc'', ''spell.episodes.care_professionals'') \nColumns:\n  professional_registration_identifier -> professional_registration_identifier\n  main_specialty -> main_specialty\n  treatment_function -> treatment_function\n  local_sub_specialty -> local_sub_specialty\n  clinical_responsibility_indicator -> clinical_responsibility_indicator\n  dmicImportLogId -> dmic_import_log_id\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  CARE_PROFESSIONALS_ID -> care_professionals_id\n  professional_registration_issuer -> professional_registration_issuer"
    )
}}
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
