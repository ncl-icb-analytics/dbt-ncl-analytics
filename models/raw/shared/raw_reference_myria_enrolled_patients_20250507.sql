{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MYRIA_ENROLLED_PATIENTS_20250507 \ndbt: source(''reference_analyst_managed'', ''MYRIA_ENROLLED_PATIENTS_20250507'') \nColumns:\n  HX_ID -> hx_id\n  ACTIVATED_DATE -> activated_date\n  ENROLLED_DATE -> enrolled_date\n  ONBOARDED_DATE -> onboarded_date\n  DISCHARGED_DATE -> discharged_date"
    )
}}
select
    "HX_ID" as hx_id,
    "ACTIVATED_DATE" as activated_date,
    "ENROLLED_DATE" as enrolled_date,
    "ONBOARDED_DATE" as onboarded_date,
    "DISCHARGED_DATE" as discharged_date
from {{ source('reference_analyst_managed', 'MYRIA_ENROLLED_PATIENTS_20250507') }}
