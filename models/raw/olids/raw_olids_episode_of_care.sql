{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.EPISODE_OF_CARE \ndbt: source(''olids'', ''EPISODE_OF_CARE'') \nColumns:\n  LDS_RECORD_ID -> lds_record_id\n  ID -> id\n  ORGANISATION_ID -> organisation_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  EPISODE_TYPE_SOURCE_CONCEPT_ID -> episode_type_source_concept_id\n  EPISODE_TYPE_SOURCE_CODE -> episode_type_source_code\n  EPISODE_TYPE_SOURCE_DISPLAY -> episode_type_source_display\n  EPISODE_TYPE_CODE -> episode_type_code\n  EPISODE_TYPE_DISPLAY -> episode_type_display\n  EPISODE_STATUS_SOURCE_CONCEPT_ID -> episode_status_source_concept_id\n  EPISODE_STATUS_SOURCE_CODE -> episode_status_source_code\n  EPISODE_STATUS_SOURCE_DISPLAY -> episode_status_source_display\n  EPISODE_STATUS_CODE -> episode_status_code\n  EPISODE_STATUS_DISPLAY -> episode_status_display\n  EPISODE_OF_CARE_START_DATE -> episode_of_care_start_date\n  EPISODE_OF_CARE_END_DATE -> episode_of_care_end_date\n  CARE_MANAGER_PRACTITIONER_ID -> care_manager_practitioner_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  RECORD_OWNER_ORGANISATION_CODE -> record_owner_organisation_code\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "ORGANISATION_ID" as organisation_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "EPISODE_TYPE_SOURCE_CONCEPT_ID" as episode_type_source_concept_id,
    "EPISODE_TYPE_SOURCE_CODE" as episode_type_source_code,
    "EPISODE_TYPE_SOURCE_DISPLAY" as episode_type_source_display,
    "EPISODE_TYPE_CODE" as episode_type_code,
    "EPISODE_TYPE_DISPLAY" as episode_type_display,
    "EPISODE_STATUS_SOURCE_CONCEPT_ID" as episode_status_source_concept_id,
    "EPISODE_STATUS_SOURCE_CODE" as episode_status_source_code,
    "EPISODE_STATUS_SOURCE_DISPLAY" as episode_status_source_display,
    "EPISODE_STATUS_CODE" as episode_status_code,
    "EPISODE_STATUS_DISPLAY" as episode_status_display,
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
from {{ source('olids', 'EPISODE_OF_CARE') }}
