-- Raw layer model for reference_terminology.LTC_LCS_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description
from {{ source('reference_terminology', 'LTC_LCS_CODES') }}
