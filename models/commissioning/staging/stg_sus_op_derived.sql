-- Staging model for sus_op.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
{% if source.get('description') %}
-- Description: SUS outpatient appointments and activity
{% endif %}

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicLSOA2021" as dmiclsoa2021,
    "dmicSubICBRegistrationSubmitted" as dmicsubicbregistrationsubmitted,
    "dmicICBResidenceSubmitted" as dmicicbresidencesubmitted,
    "CqcCareHomeCode" as cqccarehomecode,
    "dmicICBCommissioner" as dmicicbcommissioner,
    "dmicSubICBResidenceSubmitted" as dmicsubicbresidencesubmitted,
    "dmicCommissionerDerivationReason" as dmiccommissionerderivationreason,
    "dmicElectoralWardCode" as dmicelectoralwardcode,
    "dmicSubICBCommissioner" as dmicsubicbcommissioner,
    "dmicICBRegistrationSubmitted" as dmicicbregistrationsubmitted
from {{ source('sus_op', 'derived') }}
