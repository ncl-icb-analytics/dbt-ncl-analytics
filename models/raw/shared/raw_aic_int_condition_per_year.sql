-- Raw layer model for aic.INT_CONDITION_PER_YEAR
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "CONDITION_PER_YEAR_ID" as condition_per_year_id,
    "PERSON_ID" as person_id,
    "CONDITION_YEAR" as condition_year,
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "CODES" as codes,
    "CODE_DESCRIPTIONS" as code_descriptions,
    "VOCABULARIES" as vocabularies,
    "IS_FROM_GP" as is_from_gp,
    "IS_FROM_APC" as is_from_apc,
    "IS_FROM_OP" as is_from_op,
    "TOTAL_RECORDS" as total_records
from {{ source('aic', 'INT_CONDITION_PER_YEAR') }}
