{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PERSON \ndbt: source(''olids'', ''PERSON'') \nColumns:\n  ID -> id\n  PERSON_UUID -> person_uuid\n  MATCHED_NHS_NO_HASH -> matched_nhs_no_hash\n  GENDER -> gender\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month\n  COMPOSITE_ID -> composite_id\n  DEATH_NOTIFICATION_STATUS -> death_notification_status\n  POSTCODE_HASH -> postcode_hash\n  PREFERRED_CONTACT_METHOD -> preferred_contact_method\n  NOMINATED_PHARMACY -> nominated_pharmacy\n  DISPENSING_DOCTOR -> dispensing_doctor\n  MEDICAL_APPLIANCE_SUPPLIER -> medical_appliance_supplier\n  GP_PRACTICE_CODE -> gp_practice_code\n  GP_REGISTRATION_DATE -> gp_registration_date\n  AS_AT_DATE -> as_at_date\n  SENSITIVITY_FLAG -> sensitivity_flag\n  ERROR_SUCCESS_CODE -> error_success_code\n  LDS_RECORD_ID -> lds_record_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "ID" as id,
    "PERSON_UUID" as person_uuid,
    "MATCHED_NHS_NO_HASH" as matched_nhs_no_hash,
    "GENDER" as gender,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "COMPOSITE_ID" as composite_id,
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
    "LDS_RECORD_ID" as lds_record_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PERSON') }}
