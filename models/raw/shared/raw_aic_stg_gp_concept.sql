-- Raw layer model for aic.STG_GP__CONCEPT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DB_CONCEPT_ID" as db_concept_id,
    "DB_CONCEPT_ID_TYPE" as db_concept_id_type,
    "CONCEPT_CODE" as concept_code,
    "CONCEPT_NAME" as concept_name,
    "CONCEPT_VOCABULARY" as concept_vocabulary,
    "CONCEPT_SYSTEM" as concept_system,
    "VALID_FROM" as valid_from,
    "VALID_TO" as valid_to
from {{ source('aic', 'STG_GP__CONCEPT') }}
