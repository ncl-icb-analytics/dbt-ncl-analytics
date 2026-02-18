{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__RADIOTHERAPY_MODELLING_CORE_HOURS \ndbt: source(''reference_analyst_managed'', ''CANCER__RADIOTHERAPY_MODELLING_CORE_HOURS'') \nColumns:\n  PROVIDER_NAME -> provider_name\n  MACHINE_NAME -> machine_name\n  START_DATE -> start_date\n  END_DATE -> end_date\n  START_TIME -> start_time\n  END_TIME -> end_time\n  MONDAY -> monday\n  TUESDAY -> tuesday\n  WEDNESDAY -> wednesday\n  THURSDAY -> thursday\n  FRIDAY -> friday\n  SATURDAY -> saturday\n  SUNDAY -> sunday"
    )
}}
select
    "PROVIDER_NAME" as provider_name,
    "MACHINE_NAME" as machine_name,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "START_TIME" as start_time,
    "END_TIME" as end_time,
    "MONDAY" as monday,
    "TUESDAY" as tuesday,
    "WEDNESDAY" as wednesday,
    "THURSDAY" as thursday,
    "FRIDAY" as friday,
    "SATURDAY" as saturday,
    "SUNDAY" as sunday
from {{ source('reference_analyst_managed', 'CANCER__RADIOTHERAPY_MODELLING_CORE_HOURS') }}
