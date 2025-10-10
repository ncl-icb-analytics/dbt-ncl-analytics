-- Raw layer model for dictionary_eRS.E-Referral_Reason
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups
-- This is a 1:1 passthrough from source with standardized column names
select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display,
    "Usage" as usage
from {{ source('dictionary_eRS', 'E-Referral_Reason') }}
