-- Staging model for dictionary_eRS.ServicePollingDayOfWeek
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "Service_Id" as service_id,
    "PollingDayOfWeek" as pollingdayofweek
from {{ source('dictionary_eRS', 'ServicePollingDayOfWeek') }}
