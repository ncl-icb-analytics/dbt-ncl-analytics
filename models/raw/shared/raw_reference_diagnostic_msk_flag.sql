{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.DIAGNOSTIC_MSK_FLAG \ndbt: source(''reference_analyst_managed'', ''DIAGNOSTIC_MSK_FLAG'') \nColumns:\n  NICIP_CODE -> nicip_code\n  NICIP_DESCRIPTION -> nicip_description\n  MODALITY -> modality\n  EXCLUDED -> excluded"
    )
}}
select
    "NICIP_CODE" as nicip_code,
    "NICIP_DESCRIPTION" as nicip_description,
    "MODALITY" as modality,
    "EXCLUDED" as excluded
from {{ source('reference_analyst_managed', 'DIAGNOSTIC_MSK_FLAG') }}
