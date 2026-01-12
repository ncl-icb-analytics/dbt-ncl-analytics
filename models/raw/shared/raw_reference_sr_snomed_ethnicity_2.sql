{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.SR_SNOMED_ETHNICITY_2 \ndbt: source(''reference_analyst_managed'', ''SR_SNOMED_ETHNICITY_2'') \nColumns:\n  SNOMED_CODE -> snomed_code\n  ETHNICITY -> ethnicity\n  GROUPING_16 -> grouping_16\n  GROUPING_6 -> grouping_6\n  SK_ETHNICITY_ID -> sk_ethnicity_id"
    )
}}
select
    "SNOMED_CODE" as snomed_code,
    "ETHNICITY" as ethnicity,
    "GROUPING_16" as grouping_16,
    "GROUPING_6" as grouping_6,
    "SK_ETHNICITY_ID" as sk_ethnicity_id
from {{ source('reference_analyst_managed', 'SR_SNOMED_ETHNICITY_2') }}
