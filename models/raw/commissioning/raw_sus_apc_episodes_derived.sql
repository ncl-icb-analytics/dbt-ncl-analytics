{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.episodes.derived \ndbt: source(''sus_apc'', ''episodes.derived'') \nColumns:\n  dmicICBResidenceSubmitted -> dmic_icb_residence_submitted\n  dmicCommissionerDerivationReason -> dmic_commissioner_derivation_reason\n  dmicLSOA2021 -> dmic_lsoa2021\n  dmicElectoralWardCode -> dmic_electoral_ward_code\n  dmicSubICBCommissioner -> dmic_sub_icb_commissioner\n  dmicICBRegistrationSubmitted -> dmic_icb_registration_submitted\n  EPISODES_ID -> episodes_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSubICBRegistrationSubmitted -> dmic_sub_icb_registration_submitted\n  dmicSubICBResidenceSubmitted -> dmic_sub_icb_residence_submitted\n  CqcCareHomeCode -> cqc_care_home_code\n  dmicICBCommissioner -> dmic_icb_commissioner\n  PRIMARYKEY_ID -> primarykey_id"
    )
}}
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
