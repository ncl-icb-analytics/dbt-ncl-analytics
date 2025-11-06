-- Raw layer model for reference_analyst_managed.CONSULTANT_CONNECT_MESSAGES
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATE" as date,
    "TIME" as time,
    "CREATED_BY" as created_by,
    "ORGANISATION" as organisation,
    "PCN" as pcn,
    "LOCALITY" as locality,
    "ICS" as ics,
    "NO_OF_PHOTOS" as no_of_photos,
    "SHARED_WITH" as shared_with,
    "CONSULTANT" as consultant,
    "TRUST" as trust,
    "RESPONSE_TIME" as response_time,
    "RESPONSE_TIME_SEC" as response_time_sec,
    "OUTCOME" as outcome,
    "DATE_CLOSED" as date_closed,
    "LAST_ACTIVITY" as last_activity
from {{ source('reference_analyst_managed', 'CONSULTANT_CONNECT_MESSAGES') }}
