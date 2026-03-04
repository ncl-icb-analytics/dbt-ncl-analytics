{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.TEST \ndbt: source(''reference_analyst_managed'', ''TEST'') \nColumns:\n  REPORTED_MONTH -> reported_month"
    )
}}
select
    "REPORTED_MONTH" as reported_month
from {{ source('reference_analyst_managed', 'TEST') }}
