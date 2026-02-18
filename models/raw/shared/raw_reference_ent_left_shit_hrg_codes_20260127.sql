{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.ENT_LEFT_SHIT_HRG_CODES_20260127 \ndbt: source(''reference_analyst_managed'', ''ENT_LEFT_SHIT_HRG_CODES_20260127'') \nColumns:\n  HRG_CODE -> hrg_code"
    )
}}
select
    "HRG_CODE" as hrg_code
from {{ source('reference_analyst_managed', 'ENT_LEFT_SHIT_HRG_CODES_20260127') }}
