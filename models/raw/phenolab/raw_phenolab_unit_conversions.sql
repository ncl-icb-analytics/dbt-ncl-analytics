-- Raw layer model for phenolab.UNIT_CONVERSIONS
-- Source: "DATA_LAKE__NCL"."PHENOLAB_DEV"
-- Description: Phenolab supporting data
-- This is a 1:1 passthrough from source with standardized column names
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "CONVERT_FROM_UNIT" as convert_from_unit,
    "CONVERT_TO_UNIT" as convert_to_unit,
    "PRE_OFFSET" as pre_offset,
    "MULTIPLY_BY" as multiply_by,
    "POST_OFFSET" as post_offset
from {{ source('phenolab', 'UNIT_CONVERSIONS') }}
