-- Raw layer model for sus_op.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicLSOA2021" as dmic_lsoa2021,
    "dmicSubICBRegistrationSubmitted" as dmic_sub_icb_registration_submitted,
    "dmicICBResidenceSubmitted" as dmic_icb_residence_submitted,
    "CqcCareHomeCode" as cqc_care_home_code,
    "dmicICBCommissioner" as dmic_icb_commissioner,
    "dmicSubICBResidenceSubmitted" as dmic_sub_icb_residence_submitted,
    "dmicCommissionerDerivationReason" as dmic_commissioner_derivation_reason,
    "dmicElectoralWardCode" as dmic_electoral_ward_code,
    "dmicSubICBCommissioner" as dmic_sub_icb_commissioner,
    "dmicICBRegistrationSubmitted" as dmic_icb_registration_submitted
from {{ source('sus_op', 'derived') }}
