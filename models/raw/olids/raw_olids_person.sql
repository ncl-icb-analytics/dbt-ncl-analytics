{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PERSON \ndbt: source(''olids'', ''PERSON'') \nColumns:\n  ID -> id\n  PERSON_UUID -> person_uuid\n  PERSON_VERSION_ID -> person_version_id\n  PERSON_RECORD_TYPE -> person_record_type\n  MATCHED_NHS_NO_HASH -> matched_nhs_no_hash\n  SK_PATIENT_ID -> sk_patient_id\n  GENDER -> gender\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month\n  DEATH_NOTIFICATION_STATUS -> death_notification_status\n  POSTCODE_HASH -> postcode_hash\n  PREFERRED_CONTACT_METHOD -> preferred_contact_method\n  NOMINATED_PHARMACY -> nominated_pharmacy\n  DISPENSING_DOCTOR -> dispensing_doctor\n  MEDICAL_APPLIANCE_SUPPLIER -> medical_appliance_supplier\n  GP_PRACTICE_CODE -> gp_practice_code\n  GP_REGISTRATION_DATE -> gp_registration_date\n  AS_AT_DATE -> as_at_date\n  SENSITIVITY_FLAG -> sensitivity_flag\n  ERROR_SUCCESS_CODE -> error_success_code\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_DATETIME_FIRST_ACQUIRED_PERSON -> lds_datetime_first_acquired_person\n  LDS_DATETIME_UPDATE_ACQUIRED_PERSON -> lds_datetime_update_acquired_person\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "ID" as id,
    "PERSON_UUID" as person_uuid,
    "PERSON_VERSION_ID" as person_version_id,
    "PERSON_RECORD_TYPE" as person_record_type,
    "MATCHED_NHS_NO_HASH" as matched_nhs_no_hash,
    "SK_PATIENT_ID" as sk_patient_id,
    "GENDER" as gender,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "DEATH_NOTIFICATION_STATUS" as death_notification_status,
    "POSTCODE_HASH" as postcode_hash,
    "PREFERRED_CONTACT_METHOD" as preferred_contact_method,
    "NOMINATED_PHARMACY" as nominated_pharmacy,
    "DISPENSING_DOCTOR" as dispensing_doctor,
    "MEDICAL_APPLIANCE_SUPPLIER" as medical_appliance_supplier,
    "GP_PRACTICE_CODE" as gp_practice_code,
    "GP_REGISTRATION_DATE" as gp_registration_date,
    "AS_AT_DATE" as as_at_date,
    "SENSITIVITY_FLAG" as sensitivity_flag,
    "ERROR_SUCCESS_CODE" as error_success_code,
    "PERSON_SHARD_ID" as person_shard_id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "LDS_SOURCE_RECORD_SHARD_ID" as lds_source_record_shard_id,
    "LDS_ID" as lds_id,
    "LDS_SOURCE_DATASET_ID" as lds_source_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_DATETIME_FIRST_ACQUIRED_PERSON" as lds_datetime_first_acquired_person,
    "LDS_DATETIME_UPDATE_ACQUIRED_PERSON" as lds_datetime_update_acquired_person,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PERSON') }}
