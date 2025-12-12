-- Raw layer model for reference_analyst_managed.SR_SNOMED_ETHNICITY_2
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "SNOMED_CODE" as snomed_code,
    "ETHNICITY" as ethnicity,
    "GROUPING_16" as grouping_16,
    "GROUPING_6" as grouping_6,
    "SK_ETHNICITY_ID" as sk_ethnicity_id
from {{ source('reference_analyst_managed', 'SR_SNOMED_ETHNICITY_2') }}
