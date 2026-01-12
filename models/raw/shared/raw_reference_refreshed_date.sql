{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.REFRESHED_DATE \ndbt: source(''reference_analyst_managed'', ''REFRESHED_DATE'') \nColumns:\n  REFRESHED_DATE -> refreshed_date\n  PRACTICE_COUNT -> practice_count"
    )
}}
select
    "REFRESHED_DATE" as refreshed_date,
    "PRACTICE_COUNT" as practice_count
from {{ source('reference_analyst_managed', 'REFRESHED_DATE') }}
