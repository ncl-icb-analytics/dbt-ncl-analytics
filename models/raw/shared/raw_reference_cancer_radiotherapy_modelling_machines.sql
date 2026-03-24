{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__RADIOTHERAPY_MODELLING_MACHINES \ndbt: source(''reference_analyst_managed'', ''CANCER__RADIOTHERAPY_MODELLING_MACHINES'') \nColumns:\n  MACHINE_ID -> machine_id\n  MACHINE_NAME -> machine_name\n  PROVIDER_NAME -> provider_name"
    )
}}
select
    "MACHINE_ID" as machine_id,
    "MACHINE_NAME" as machine_name,
    "PROVIDER_NAME" as provider_name
from {{ source('reference_analyst_managed', 'CANCER__RADIOTHERAPY_MODELLING_MACHINES') }}
