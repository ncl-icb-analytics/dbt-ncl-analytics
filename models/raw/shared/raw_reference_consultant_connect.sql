-- Raw layer model for reference_analyst_managed.CONSULTANT_CONNECT
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Date" as date,
    "Time" as time,
    "User" as user,
    "Organisation" as organisation,
    "GP_Code" as gp_code,
    "GPPracticeName" as gp_practice_name,
    "grand_total_weighted_list_size" as grand_total_weighted_list_size,
    "borough" as borough,
    "PCN" as pcn,
    "CCG" as ccg,
    "ICS" as ics,
    "Photos" as photos,
    "Shared with" as shared_with,
    "Consultant" as consultant,
    "Trust" as trust,
    "ResponseTimeFormatted" as response_time_formatted,
    "ResponseTimeSeconds" as response_time_seconds,
    "WaitTimeSeconds" as wait_time_seconds,
    "TalkTimeSeconds" as talk_time_seconds,
    "Specialism" as specialism,
    "ServiceType" as service_type,
    "RotaPosition" as rota_position,
    "Outcome" as outcome,
    "DateClosed" as date_closed,
    "LastActivity" as last_activity,
    "ContactType" as contact_type,
    "Activity" as activity
from {{ source('reference_analyst_managed', 'CONSULTANT_CONNECT') }}
