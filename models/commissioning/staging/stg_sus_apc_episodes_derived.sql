-- Staging model for sus_apc.episodes.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "dmicICBResidenceSubmitted" as dmic_icb_residence_submitted,
    "dmicCommissionerDerivationReason" as dmic_commissioner_derivation_reason,
    "dmicLSOA2021" as dmic_lsoa2021,
    "dmicElectoralWardCode" as dmic_electoral_ward_code,
    "dmicSubICBCommissioner" as dmic_sub_icb_commissioner,
    "dmicICBRegistrationSubmitted" as dmic_icb_registration_submitted,
    "EPISODES_ID" as episodes_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSubICBRegistrationSubmitted" as dmic_sub_icb_registration_submitted,
    "dmicSubICBResidenceSubmitted" as dmic_sub_icb_residence_submitted,
    "CqcCareHomeCode" as cqc_care_home_code,
    "dmicICBCommissioner" as dmic_icb_commissioner,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_apc', 'episodes.derived') }}
