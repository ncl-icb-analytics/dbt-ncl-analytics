-- Raw layer model for phenolab.BASE_ATHENA__CONCEPT
-- Source: "DATA_LAKE__NCL"."PHENOLAB_DEV"
-- Description: Phenolab supporting data
-- This is a 1:1 passthrough from source with standardized column names
select
    "CONCEPT_ID" as concept_id,
    "CONCEPT_NAME" as concept_name,
    "DOMAIN_ID" as domain_id,
    "VOCABULARY_ID" as vocabulary_id,
    "CONCEPT_CLASS_ID" as concept_class_id,
    "STANDARD_CONCEPT" as standard_concept,
    "CONCEPT_CODE" as concept_code,
    "VALID_START_DATE" as valid_start_date,
    "VALID_END_DATE" as valid_end_date,
    "INVALID_REASON" as invalid_reason
from {{ source('phenolab', 'BASE_ATHENA__CONCEPT') }}
