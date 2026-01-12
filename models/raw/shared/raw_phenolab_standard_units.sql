{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.STANDARD_UNITS \ndbt: source(''phenolab'', ''STANDARD_UNITS'') \nColumns:\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  CONFIG_ID -> config_id\n  CONFIG_VERSION -> config_version\n  UNIT -> unit\n  PRIMARY_UNIT -> primary_unit"
    )
}}
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "UNIT" as unit,
    "PRIMARY_UNIT" as primary_unit
from {{ source('phenolab', 'STANDARD_UNITS') }}
