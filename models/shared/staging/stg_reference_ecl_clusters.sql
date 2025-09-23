-- Staging model for reference_terminology.ECL_CLUSTERS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

select
    "CLUSTER_ID" as cluster_id,
    "ECL_EXPRESSION" as ecl_expression,
    "DESCRIPTION" as description,
    "CREATED_AT" as created_at,
    "UPDATED_AT" as updated_at,
    "CREATED_BY" as created_by,
    "UPDATED_BY" as updated_by,
    "CLUSTER_TYPE" as cluster_type
from {{ source('reference_terminology', 'ECL_CLUSTERS') }}
