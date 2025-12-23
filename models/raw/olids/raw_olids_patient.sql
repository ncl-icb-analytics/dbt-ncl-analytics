-- Raw layer model for olids.PATIENT
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
-- This is a 1:1 passthrough from source with standardized column names
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "NHS_NUMBER_HASH" as nhs_number_hash,
    "SK_PATIENT_ID" as sk_patient_id,
    "TITLE" as title,
    "GENDER_CONCEPT_ID" as gender_concept_id,
    "GENDER_SOURCE_CODE" as gender_source_code,
    "GENDER_SOURCE_DISPLAY" as gender_source_display,
    "GENDER_CODE" as gender_code,
    "GENDER_DISPLAY" as gender_display,
    "REGISTERED_PRACTICE_ID" as registered_practice_id,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "IS_SPINE_SENSITIVE" as is_spine_sensitive,
    "IS_CONFIDENTIAL" as is_confidential,
    "IS_DUMMY_PATIENT" as is_dummy_patient,
    "RECORD_OWNER_ORGANISATION_CODE" as record_owner_organisation_code,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT') }}
