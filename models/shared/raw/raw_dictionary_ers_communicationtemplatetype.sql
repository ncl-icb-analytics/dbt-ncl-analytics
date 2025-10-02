-- Raw layer model for dictionary_eRS.CommunicationTemplateType
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups
-- This is a 1:1 passthrough from source with standardized column names
select
    "Code" as code,
    "Meaning" as meaning,
    "Display" as display
from {{ source('dictionary_eRS', 'CommunicationTemplateType') }}
