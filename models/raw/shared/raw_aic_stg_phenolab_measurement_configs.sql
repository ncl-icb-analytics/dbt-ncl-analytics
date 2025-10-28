-- Raw layer model for aic.STG_PHENOLAB__MEASUREMENT_CONFIGS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version
from {{ source('aic', 'STG_PHENOLAB__MEASUREMENT_CONFIGS') }}
