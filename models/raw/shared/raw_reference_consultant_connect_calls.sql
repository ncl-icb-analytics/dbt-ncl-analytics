{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CONSULTANT_CONNECT_CALLS \ndbt: source(''reference_analyst_managed'', ''CONSULTANT_CONNECT_CALLS'') \nColumns:\n  DATE -> date\n  TIME -> time\n  CALLER -> caller\n  ORGANISATION -> organisation\n  LOCALITY -> locality\n  ICS -> ics\n  PCN -> pcn\n  SPECIALISM -> specialism\n  CONSULTANT -> consultant\n  TRUST -> trust\n  SERVICE_TYPE -> service_type\n  ROTA_POSITION -> rota_position\n  WAIT_TIME_SEC -> wait_time_sec\n  TALK_TIME_SEC -> talk_time_sec\n  OUTCOME -> outcome"
    )
}}
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
