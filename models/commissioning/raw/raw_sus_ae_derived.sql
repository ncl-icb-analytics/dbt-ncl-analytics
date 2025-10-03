-- Raw layer model for sus_ae.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "dmicSubICBRegistrationSubmitted" as dmic_sub_icb_registration_submitted,
    "dmicElectoralWardCode" as dmic_electoral_ward_code,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicSubICBResidenceSubmitted" as dmic_sub_icb_residence_submitted,
    "dmicCommissionerDerivationReason" as dmic_commissioner_derivation_reason,
    "CqcCareHomeCode" as cqc_care_home_code,
    "dmicICBCommissioner" as dmic_icb_commissioner,
    "dmicSubICBCommissioner" as dmic_sub_icb_commissioner,
    "dmicICBRegistrationSubmitted" as dmic_icb_registration_submitted,
    "dmicICBResidenceSubmitted" as dmic_icb_residence_submitted,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicLSOA2021" as dmic_lsoa2021
from {{ source('sus_ae', 'derived') }}
