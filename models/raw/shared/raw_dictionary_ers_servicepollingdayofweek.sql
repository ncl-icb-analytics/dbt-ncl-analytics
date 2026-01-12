{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.ServicePollingDayOfWeek \ndbt: source(''dictionary_eRS'', ''ServicePollingDayOfWeek'') \nColumns:\n  Service_Id -> service_id\n  PollingDayOfWeek -> polling_day_of_week"
    )
}}
select
    "Service_Id" as service_id,
    "PollingDayOfWeek" as polling_day_of_week
from {{ source('dictionary_eRS', 'ServicePollingDayOfWeek') }}
