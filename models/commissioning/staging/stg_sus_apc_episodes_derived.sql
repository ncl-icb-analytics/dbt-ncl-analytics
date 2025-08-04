-- Staging model for sus_apc.episodes.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "dmicICBResidenceSubmitted" as dmicicbresidencesubmitted,
    "dmicCommissionerDerivationReason" as dmiccommissionerderivationreason,
    "dmicLSOA2021" as dmiclsoa2021,
    "dmicElectoralWardCode" as dmicelectoralwardcode,
    "dmicSubICBCommissioner" as dmicsubicbcommissioner,
    "dmicICBRegistrationSubmitted" as dmicicbregistrationsubmitted,
    "EPISODES_ID" as episodes_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicSubICBRegistrationSubmitted" as dmicsubicbregistrationsubmitted,
    "dmicSubICBResidenceSubmitted" as dmicsubicbresidencesubmitted,
    "CqcCareHomeCode" as cqccarehomecode,
    "dmicICBCommissioner" as dmicicbcommissioner,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_apc', 'episodes.derived') }}
