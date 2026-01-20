{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.UNIT_MAPPINGS \ndbt: source(''phenolab'', ''UNIT_MAPPINGS'') \nColumns:\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  CONFIG_ID -> config_id\n  CONFIG_VERSION -> config_version\n  SOURCE_UNIT -> source_unit\n  STANDARD_UNIT -> standard_unit\n  SOURCE_UNIT_COUNT -> source_unit_count\n  SOURCE_UNIT_LQ -> source_unit_lq\n  SOURCE_UNIT_MEDIAN -> source_unit_median\n  SOURCE_UNIT_UQ -> source_unit_uq"
    )
}}
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
from {{ source('phenolab', 'UNIT_MAPPINGS') }}
