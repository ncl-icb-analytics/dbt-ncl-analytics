{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT \ndbt: source(''olids'', ''PATIENT'') \nColumns:\n  LDS_RECORD_ID -> lds_record_id\n  ID -> id\n  NHS_NUMBER_HASH -> nhs_number_hash\n  SK_PATIENT_ID -> sk_patient_id\n  TITLE -> title\n  GENDER_CONCEPT_ID -> gender_concept_id\n  GENDER_SOURCE_CODE -> gender_source_code\n  GENDER_SOURCE_DISPLAY -> gender_source_display\n  GENDER_CODE -> gender_code\n  GENDER_DISPLAY -> gender_display\n  REGISTERED_PRACTICE_ID -> registered_practice_id\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month\n  IS_SPINE_SENSITIVE -> is_spine_sensitive\n  IS_CONFIDENTIAL -> is_confidential\n  IS_DUMMY_PATIENT -> is_dummy_patient\n  RECORD_OWNER_ORGANISATION_CODE -> record_owner_organisation_code\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
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
