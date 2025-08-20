-- Staging model for dictionary_eRS.E-Referral_Reason
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display,
    "Usage" as usage
from {{ source('dictionary_eRS', 'E-Referral_Reason') }}
