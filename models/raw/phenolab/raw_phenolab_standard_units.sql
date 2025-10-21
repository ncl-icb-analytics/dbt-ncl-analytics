-- Raw layer model for phenolab.STANDARD_UNITS
-- Source: "DATA_LAKE__NCL"."PHENOLAB_DEV"
-- Description: Phenolab supporting data
-- This is a 1:1 passthrough from source with standardized column names
select
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CONFIG_ID" as config_id,
    "CONFIG_VERSION" as config_version,
    "UNIT" as unit,
    "PRIMARY_UNIT" as primary_unit
from {{ source('phenolab', 'STANDARD_UNITS') }}
