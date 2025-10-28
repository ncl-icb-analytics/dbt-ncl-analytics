-- Raw layer model for aic.STG_PHENOLAB__VALUE_BOUNDS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "LOWER_LIMIT" as lower_limit,
    "UPPER_LIMIT" as upper_limit
from {{ source('aic', 'STG_PHENOLAB__VALUE_BOUNDS') }}
