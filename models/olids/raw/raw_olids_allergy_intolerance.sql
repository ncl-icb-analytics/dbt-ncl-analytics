-- Raw layer model for olids.ALLERGY_INTOLERANCE
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
-- This is a 1:1 passthrough from source with standardized column names
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "PATIENT_ID" as patient_id,
    "PRACTITIONER_ID" as practitioner_id,
    "ENCOUNTER_ID" as encounter_id,
    "CLINICAL_STATUS" as clinical_status,
    "VERIFICATION_STATUS" as verification_status,
    "CATEGORY" as category,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "DATE_PRECISION_CONCEPT_ID" as date_precision_concept_id,
    "IS_REVIEW" as is_review,
    "MEDICATION_NAME" as medication_name,
    "MULTI_LEX_ACTION" as multi_lex_action,
    "ALLERGY_INTOLERANCE_SOURCE_CONCEPT_ID" as allergy_intolerance_source_concept_id,
    "AGE_AT_EVENT" as age_at_event,
    "AGE_AT_EVENT_BABY" as age_at_event_baby,
    "AGE_AT_EVENT_NEONATE" as age_at_event_neonate,
    "DATE_RECORDED" as date_recorded,
    "IS_CONFIDENTIAL" as is_confidential,
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
from {{ source('olids', 'ALLERGY_INTOLERANCE') }}
