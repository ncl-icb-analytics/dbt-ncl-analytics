{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__RADIOTHERAPY_MODELLING_EXCEPTIONS \ndbt: source(''reference_analyst_managed'', ''CANCER__RADIOTHERAPY_MODELLING_EXCEPTIONS'') \nColumns:\n  PROVIDER_NAME -> provider_name\n  MACHINE_NAME -> machine_name\n  START_DATE -> start_date\n  END_DATE -> end_date\n  OVERRIDE_START -> override_start\n  OVERRIDE_END -> override_end\n  STATUS -> status\n  NOTES -> notes\n  MONDAY -> monday\n  TUESDAY -> tuesday\n  WEDNESDAY -> wednesday\n  THURSDAY -> thursday\n  FRIDAY -> friday\n  SATURDAY -> saturday\n  SUNDAY -> sunday"
    )
}}
select
    "PROVIDER_NAME" as provider_name,
    "MACHINE_NAME" as machine_name,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "OVERRIDE_START" as override_start,
    "OVERRIDE_END" as override_end,
    "STATUS" as status,
    "NOTES" as notes,
    "MONDAY" as monday,
    "TUESDAY" as tuesday,
    "WEDNESDAY" as wednesday,
    "THURSDAY" as thursday,
    "FRIDAY" as friday,
    "SATURDAY" as saturday,
    "SUNDAY" as sunday
from {{ source('reference_analyst_managed', 'CANCER__RADIOTHERAPY_MODELLING_EXCEPTIONS') }}
