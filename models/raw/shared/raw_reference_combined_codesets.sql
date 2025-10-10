-- Raw layer model for reference_terminology.COMBINED_CODESETS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('reference_terminology', 'COMBINED_CODESETS') }}
