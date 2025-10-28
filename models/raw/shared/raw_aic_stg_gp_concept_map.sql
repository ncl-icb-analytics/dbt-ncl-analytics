-- Raw layer model for aic.STG_GP__CONCEPT_MAP
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MAP_ID" as map_id,
    "SOURCE_DB_CONCEPT_ID" as source_db_concept_id,
    "SOURCE_DB_CONCEPT_ID_TYPE" as source_db_concept_id_type,
    "SOURCE_CONCEPT_CODE" as source_concept_code,
    "SOURCE_CONCEPT_NAME" as source_concept_name,
    "SOURCE_CONCEPT_VOCABULARY" as source_concept_vocabulary,
    "TARGET_DB_CONCEPT_ID" as target_db_concept_id,
    "TARGET_DB_CONCEPT_ID_TYPE" as target_db_concept_id_type,
    "TARGET_CONCEPT_CODE" as target_concept_code,
    "TARGET_CONCEPT_NAME" as target_concept_name,
    "TARGET_CONCEPT_VOCABULARY" as target_concept_vocabulary
from {{ source('aic', 'STG_GP__CONCEPT_MAP') }}
