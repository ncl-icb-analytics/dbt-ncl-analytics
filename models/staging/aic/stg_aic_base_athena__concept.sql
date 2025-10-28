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
from {{ ref('raw_aic_base_athena_concept') }}