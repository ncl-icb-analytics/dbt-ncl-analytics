-- Raw layer model for aic.BASE_OLIDS__EPISODE_OF_CARE
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "ORGANISATION_ID" as organisation_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "EPISODE_TYPE_SOURCE_CONCEPT_ID" as episode_type_source_concept_id,
    "EPISODE_STATUS_SOURCE_CONCEPT_ID" as episode_status_source_concept_id,
    "EPISODE_OF_CARE_START_DATE" as episode_of_care_start_date,
    "EPISODE_OF_CARE_END_DATE" as episode_of_care_end_date,
    "CARE_MANAGER_PRACTITIONER_ID" as care_manager_practitioner_id,
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
from {{ source('aic', 'BASE_OLIDS__EPISODE_OF_CARE') }}
