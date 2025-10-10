-- Raw layer model for reference_terminology.PCD_REFSET_LATEST
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_CODE_DESCRIPTION" as snomed_code_description,
    "PCD_REFSET_ID" as pcd_refset_id,
    "SERVICE_AND_RULESET" as service_and_ruleset
from {{ source('reference_terminology', 'PCD_REFSET_LATEST') }}
