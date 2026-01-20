{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CONSULTANT_CONNECT_MESSAGES \ndbt: source(''reference_analyst_managed'', ''CONSULTANT_CONNECT_MESSAGES'') \nColumns:\n  DATE -> date\n  TIME -> time\n  CREATED_BY -> created_by\n  ORGANISATION -> organisation\n  PCN -> pcn\n  LOCALITY -> locality\n  ICS -> ics\n  NO_OF_PHOTOS -> no_of_photos\n  SHARED_WITH -> shared_with\n  CONSULTANT -> consultant\n  TRUST -> trust\n  RESPONSE_TIME -> response_time\n  RESPONSE_TIME_SEC -> response_time_sec\n  OUTCOME -> outcome\n  DATE_CLOSED -> date_closed\n  LAST_ACTIVITY -> last_activity"
    )
}}
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
