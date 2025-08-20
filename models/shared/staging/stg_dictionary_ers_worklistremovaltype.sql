-- Staging model for dictionary_eRS.WorklistRemovalType
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display
from {{ source('dictionary_eRS', 'WorklistRemovalType') }}
