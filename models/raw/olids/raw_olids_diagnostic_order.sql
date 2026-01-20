{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.DIAGNOSTIC_ORDER \ndbt: source(''olids'', ''DIAGNOSTIC_ORDER'') \nColumns:\n  LDS_RECORD_ID -> lds_record_id\n  ID -> id\n  PATIENT_ID -> patient_id\n  ENCOUNTER_ID -> encounter_id\n  PRACTITIONER_ID -> practitioner_id\n  PARENT_OBSERVATION_ID -> parent_observation_id\n  CLINICAL_EFFECTIVE_DATE -> clinical_effective_date\n  DATE_PRECISION_CONCEPT_ID -> date_precision_concept_id\n  RESULT_VALUE -> result_value\n  RESULT_VALUE_UNITS_CONCEPT_ID -> result_value_units_concept_id\n  RESULT_DATE -> result_date\n  RESULT_TEXT -> result_text\n  IS_PROBLEM -> is_problem\n  IS_REVIEW -> is_review\n  PROBLEM_END_DATE -> problem_end_date\n  DIAGNOSTIC_ORDER_SOURCE_CONCEPT_ID -> diagnostic_order_source_concept_id\n  AGE_AT_EVENT -> age_at_event\n  AGE_AT_EVENT_BABY -> age_at_event_baby\n  AGE_AT_EVENT_NEONATE -> age_at_event_neonate\n  EPISODICITY_CONCEPT_ID -> episodicity_concept_id\n  IS_PRIMARY -> is_primary\n  DATE_RECORDED -> date_recorded\n  PERSON_ID -> person_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  RECORD_OWNER_ORGANISATION_CODE -> record_owner_organisation_code\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "PATIENT_ID" as patient_id,
    "ENCOUNTER_ID" as encounter_id,
    "PRACTITIONER_ID" as practitioner_id,
    "PARENT_OBSERVATION_ID" as parent_observation_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "DATE_PRECISION_CONCEPT_ID" as date_precision_concept_id,
    "RESULT_VALUE" as result_value,
    "RESULT_VALUE_UNITS_CONCEPT_ID" as result_value_units_concept_id,
    "RESULT_DATE" as result_date,
    "RESULT_TEXT" as result_text,
    "IS_PROBLEM" as is_problem,
    "IS_REVIEW" as is_review,
    "PROBLEM_END_DATE" as problem_end_date,
    "DIAGNOSTIC_ORDER_SOURCE_CONCEPT_ID" as diagnostic_order_source_concept_id,
    "AGE_AT_EVENT" as age_at_event,
    "AGE_AT_EVENT_BABY" as age_at_event_baby,
    "AGE_AT_EVENT_NEONATE" as age_at_event_neonate,
    "EPISODICITY_CONCEPT_ID" as episodicity_concept_id,
    "IS_PRIMARY" as is_primary,
    "DATE_RECORDED" as date_recorded,
    "PERSON_ID" as person_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "RECORD_OWNER_ORGANISATION_CODE" as record_owner_organisation_code,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'DIAGNOSTIC_ORDER') }}
