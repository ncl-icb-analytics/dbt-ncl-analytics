-- Staging model for sus_ae.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "dmicSubICBRegistrationSubmitted" as dmicsubicbregistrationsubmitted,
    "dmicElectoralWardCode" as dmicelectoralwardcode,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicSubICBResidenceSubmitted" as dmicsubicbresidencesubmitted,
    "dmicCommissionerDerivationReason" as dmiccommissionerderivationreason,
    "CqcCareHomeCode" as cqccarehomecode,
    "dmicICBCommissioner" as dmicicbcommissioner,
    "dmicSubICBCommissioner" as dmicsubicbcommissioner,
    "dmicICBRegistrationSubmitted" as dmicicbregistrationsubmitted,
    "dmicICBResidenceSubmitted" as dmicicbresidencesubmitted,
    "dmicImportLogId" as dmicimportlogid,
    "dmicLSOA2021" as dmiclsoa2021
from {{ source('sus_ae', 'derived') }}
