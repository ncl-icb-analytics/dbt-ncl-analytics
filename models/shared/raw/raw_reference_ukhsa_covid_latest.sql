-- Raw layer model for reference_terminology.UKHSA_COVID_LATEST
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CODING_SCHEME" as coding_scheme,
    "LIBRARY" as library,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "CODE_VALIDATED" as code_validated
from {{ source('reference_terminology', 'UKHSA_COVID_LATEST') }}
