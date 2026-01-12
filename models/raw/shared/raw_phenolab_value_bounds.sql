{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.VALUE_BOUNDS \ndbt: source(''phenolab'', ''VALUE_BOUNDS'') \nColumns:\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  CONFIG_ID -> config_id\n  CONFIG_VERSION -> config_version\n  LOWER_LIMIT -> lower_limit\n  UPPER_LIMIT -> upper_limit"
    )
}}
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "LOWER_LIMIT" as lower_limit,
    "UPPER_LIMIT" as upper_limit
from {{ source('phenolab', 'VALUE_BOUNDS') }}
