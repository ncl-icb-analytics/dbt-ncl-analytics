-- Raw layer model for reference_analyst_managed.CONSULTANT_CONNECT_CALLS_MESSAGES
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
