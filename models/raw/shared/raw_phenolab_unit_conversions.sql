{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.UNIT_CONVERSIONS \ndbt: source(''phenolab'', ''UNIT_CONVERSIONS'') \nColumns:\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  CONFIG_ID -> config_id\n  CONFIG_VERSION -> config_version\n  CONVERT_FROM_UNIT -> convert_from_unit\n  CONVERT_TO_UNIT -> convert_to_unit\n  PRE_OFFSET -> pre_offset\n  MULTIPLY_BY -> multiply_by\n  POST_OFFSET -> post_offset"
    )
}}
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
