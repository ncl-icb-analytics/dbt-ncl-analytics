-- Staging model for dictionary_eRS.ServicePollingDayOfWeek
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "Service_Id" as service_id,
    "PollingDayOfWeek" as polling_day_of_week
from {{ source('dictionary_eRS', 'ServicePollingDayOfWeek') }}
