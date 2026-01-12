{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CONSULTANT_CONNECT_CALLS_MESSAGES \ndbt: source(''reference_analyst_managed'', ''CONSULTANT_CONNECT_CALLS_MESSAGES'') \nColumns:\n  DATE -> date\n  TIME -> time\n  USER -> user\n  ORGANISATION -> organisation\n  LOCALITY -> locality\n  ICS -> ics\n  PCN -> pcn\n  SPECIALISM -> specialism\n  NO_OF_PHOTOS -> no_of_photos\n  CONSULTANT -> consultant\n  TRUST -> trust\n  SERVICE_TYPE -> service_type\n  ROTA_POSITION -> rota_position\n  WAIT_TIME_SEC -> wait_time_sec\n  TALK_TIME_SEC -> talk_time_sec\n  RESPONSE_TIME -> response_time\n  RESPONSE_TIME_SEC -> response_time_sec\n  OUTCOME -> outcome\n  DATE_CLOSED -> date_closed\n  LAST_ACTIVITY -> last_activity\n  CONTACT_TYPE -> contact_type"
    )
}}
select
    "DATE" as date,
    "TIME" as time,
    "USER" as user,
    "ORGANISATION" as organisation,
    "LOCALITY" as locality,
    "ICS" as ics,
    "PCN" as pcn,
    "SPECIALISM" as specialism,
    "NO_OF_PHOTOS" as no_of_photos,
    "CONSULTANT" as consultant,
    "TRUST" as trust,
    "SERVICE_TYPE" as service_type,
    "ROTA_POSITION" as rota_position,
    "WAIT_TIME_SEC" as wait_time_sec,
    "TALK_TIME_SEC" as talk_time_sec,
    "RESPONSE_TIME" as response_time,
    "RESPONSE_TIME_SEC" as response_time_sec,
    "OUTCOME" as outcome,
    "DATE_CLOSED" as date_closed,
    "LAST_ACTIVITY" as last_activity,
    "CONTACT_TYPE" as contact_type
from {{ source('reference_analyst_managed', 'CONSULTANT_CONNECT_CALLS_MESSAGES') }}
