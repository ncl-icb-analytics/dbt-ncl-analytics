-- Staging model for sus_apc.spell.derived
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "dmicSubICBResidenceSubmitted" as dmicsubicbresidencesubmitted,
    "dmicICBResidenceSubmitted" as dmicicbresidencesubmitted,
    "dmicSubICBRegistrationSubmitted" as dmicsubicbregistrationsubmitted,
    "dmicCommissionerDerivationReason" as dmiccommissionerderivationreason,
    "dmicLSOA2021" as dmiclsoa2021,
    "dmicElectoralWardCode" as dmicelectoralwardcode,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicSubICBCommissioner" as dmicsubicbcommissioner,
    "dmicICBRegistrationSubmitted" as dmicicbregistrationsubmitted,
    "CqcCareHomeCode" as cqccarehomecode,
    "dmicICBCommissioner" as dmicicbcommissioner
from {{ source('sus_apc', 'spell.derived') }}
