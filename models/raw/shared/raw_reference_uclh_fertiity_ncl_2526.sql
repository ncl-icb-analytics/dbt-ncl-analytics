{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UCLH_FERTIITY_NCL_2526 \ndbt: source(''reference_analyst_managed'', ''UCLH_FERTIITY_NCL_2526'') \nColumns:\n  Received Yr -> received_yr\n  DATE -> date\n  DESCRIPTION -> description\n  CUSTOMER -> customer\n  VALUE -> value\n  Month of treatment text -> month_of_treatment_text\n  Month of treatment -> month_of_treatment\n  Fin Yr -> fin_yr"
    )
}}
select
    "Received Yr" as received_yr,
    "DATE" as date,
    "DESCRIPTION" as description,
    "CUSTOMER" as customer,
    "VALUE" as value,
    "Month of treatment text" as month_of_treatment_text,
    "Month of treatment" as month_of_treatment,
    "Fin Yr" as fin_yr
from {{ source('reference_analyst_managed', 'UCLH_FERTIITY_NCL_2526') }}
