-- Raw layer model for reference_analyst_managed.CONSULTANT_CONNECT_CALLS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE" as date,
    "TIME" as time,
    "CALLER" as caller,
    "ORGANISATION" as organisation,
    "LOCALITY" as locality,
    "ICS" as ics,
    "PCN" as pcn,
    "SPECIALISM" as specialism,
    "CONSULTANT" as consultant,
    "TRUST" as trust,
    "SERVICE_TYPE" as service_type,
    "ROTA_POSITION" as rota_position,
    "WAIT_TIME_SEC" as wait_time_sec,
    "TALK_TIME_SEC" as talk_time_sec,
    "OUTCOME" as outcome
from {{ source('reference_analyst_managed', 'CONSULTANT_CONNECT_CALLS') }}
