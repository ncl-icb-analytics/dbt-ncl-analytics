{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LSOA21_BOUNDARIES \ndbt: source(''reference_analyst_managed'', ''LSOA21_BOUNDARIES'') \nColumns:\n  GEO -> geo"
    )
}}
select
    "GEO" as geo
from {{ source('reference_analyst_managed', 'LSOA21_BOUNDARIES') }}
