-- Raw layer model for dictionary_eRS.ServicePollingDayOfWeek
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups
-- This is a 1:1 passthrough from source with standardized column names
select
    "Service_Id" as service_id,
    "PollingDayOfWeek" as polling_day_of_week
from {{ source('dictionary_eRS', 'ServicePollingDayOfWeek') }}
