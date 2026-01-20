{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_ATHENA__CONCEPT \ndbt: source(''aic'', ''BASE_ATHENA__CONCEPT'') \nColumns:\n  CONCEPT_ID -> concept_id\n  CONCEPT_NAME -> concept_name\n  DOMAIN_ID -> domain_id\n  VOCABULARY_ID -> vocabulary_id\n  CONCEPT_CLASS_ID -> concept_class_id\n  STANDARD_CONCEPT -> standard_concept\n  CONCEPT_CODE -> concept_code\n  VALID_START_DATE -> valid_start_date\n  VALID_END_DATE -> valid_end_date\n  INVALID_REASON -> invalid_reason"
    )
}}
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
from {{ source('aic', 'BASE_ATHENA__CONCEPT') }}
