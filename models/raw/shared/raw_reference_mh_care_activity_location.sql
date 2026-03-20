{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MH_CARE_ACTIVITY_LOCATION \ndbt: source(''reference_analyst_managed'', ''MH_CARE_ACTIVITY_LOCATION'') \nColumns:\n  CODE -> code\n  DESCRIPTION -> description"
    )
}}
select
    "CODE" as code,
    "DESCRIPTION" as description
from {{ source('reference_analyst_managed', 'MH_CARE_ACTIVITY_LOCATION') }}
