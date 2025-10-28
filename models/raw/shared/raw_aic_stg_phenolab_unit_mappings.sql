-- Raw layer model for aic.STG_PHENOLAB__UNIT_MAPPINGS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "SOURCE_UNIT" as source_unit,
    "STANDARD_UNIT" as standard_unit,
    "SOURCE_UNIT_COUNT" as source_unit_count,
    "SOURCE_UNIT_LQ" as source_unit_lq,
    "SOURCE_UNIT_MEDIAN" as source_unit_median,
    "SOURCE_UNIT_UQ" as source_unit_uq
from {{ source('aic', 'STG_PHENOLAB__UNIT_MAPPINGS') }}
