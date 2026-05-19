{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.DEV_MEASUREMENTS \ndbt: source(''phenolab'', ''DEV_MEASUREMENTS'') \nColumns:\n  PERSON_ID -> person_id\n  VISIT_OCCURRENCE_ID -> visit_occurrence_id\n  VISIT_OCCURRENCE_TYPE -> visit_occurrence_type\n  AGE_AT_EVENT -> age_at_event\n  CLINICAL_EFFECTIVE_DATE -> clinical_effective_date\n  DEFINITION_ID -> definition_id\n  MEASUREMENT_DEFINITION_NAME -> measurement_definition_name\n  DEFINITION_SOURCE -> definition_source\n  MEASUREMENT_VALUE_AS_NUMBER -> measurement_value_as_number\n  MEASUREMENT_UNIT -> measurement_unit\n  MEASUREMENT_SOURCE_VALUE -> measurement_source_value\n  MEASUREMENT_SOURCE_UNIT -> measurement_source_unit\n  LOWER_LIMIT -> lower_limit\n  UPPER_LIMIT -> upper_limit\n  NO_UNIT_MAPPING -> no_unit_mapping\n  BELOW_BOUND -> below_bound\n  ABOVE_BOUND -> above_bound"
    )
}}
select
    "PERSON_ID" as person_id,
    "VISIT_OCCURRENCE_ID" as visit_occurrence_id,
    "VISIT_OCCURRENCE_TYPE" as visit_occurrence_type,
    "AGE_AT_EVENT" as age_at_event,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "DEFINITION_ID" as definition_id,
    "MEASUREMENT_DEFINITION_NAME" as measurement_definition_name,
    "DEFINITION_SOURCE" as definition_source,
    "MEASUREMENT_VALUE_AS_NUMBER" as measurement_value_as_number,
    "MEASUREMENT_UNIT" as measurement_unit,
    "MEASUREMENT_SOURCE_VALUE" as measurement_source_value,
    "MEASUREMENT_SOURCE_UNIT" as measurement_source_unit,
    "LOWER_LIMIT" as lower_limit,
    "UPPER_LIMIT" as upper_limit,
    "NO_UNIT_MAPPING" as no_unit_mapping,
    "BELOW_BOUND" as below_bound,
    "ABOVE_BOUND" as above_bound
from {{ source('phenolab', 'DEV_MEASUREMENTS') }}
