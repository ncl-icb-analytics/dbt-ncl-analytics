{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT \ndbt: source(''olids'', ''PATIENT'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  PERSON_ID -> person_id\n  NHS_NUMBER_HASH -> nhs_number_hash\n  SK_PATIENT_ID -> sk_patient_id\n  LOCAL_PATIENT_ID -> local_patient_id\n  TITLE -> title\n  GENDER_SOURCE_CONCEPT_ID -> gender_source_concept_id\n  GENDER_SOURCE_CODE -> gender_source_code\n  GENDER_SOURCE_DISPLAY -> gender_source_display\n  GENDER_CODE -> gender_code\n  GENDER_DISPLAY -> gender_display\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  REGISTERED_PRACTICE_ORGANISATION_ID -> registered_practice_organisation_id\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month\n  IS_CONFIDENTIAL -> is_confidential\n  IS_TEST_PATIENT -> is_test_patient\n  IS_SPINE_SENSITIVE -> is_spine_sensitive\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  PATIENT_SHARD_ID -> patient_shard_id\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_FIRST_ACQUIRED -> lds_datetime_first_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "PERSON_ID" as person_id,
    "NHS_NUMBER_HASH" as nhs_number_hash,
    "SK_PATIENT_ID" as sk_patient_id,
    "LOCAL_PATIENT_ID" as local_patient_id,
    "TITLE" as title,
    "GENDER_SOURCE_CONCEPT_ID" as gender_source_concept_id,
    "GENDER_SOURCE_CODE" as gender_source_code,
    "GENDER_SOURCE_DISPLAY" as gender_source_display,
    "GENDER_CODE" as gender_code,
    "GENDER_DISPLAY" as gender_display,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "REGISTERED_PRACTICE_ORGANISATION_ID" as registered_practice_organisation_id,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "IS_CONFIDENTIAL" as is_confidential,
    "IS_TEST_PATIENT" as is_test_patient,
    "IS_SPINE_SENSITIVE" as is_spine_sensitive,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "PATIENT_SHARD_ID" as patient_shard_id,
    "PERSON_SHARD_ID" as person_shard_id,
    "LDS_SOURCE_RECORD_SHARD_ID" as lds_source_record_shard_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_SOURCE_DATASET_ID" as lds_source_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "LDS_DATETIME_FIRST_ACQUIRED" as lds_datetime_first_acquired,
    "LDS_DATETIME_UPDATE_ACQUIRED" as lds_datetime_update_acquired,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT') }}
