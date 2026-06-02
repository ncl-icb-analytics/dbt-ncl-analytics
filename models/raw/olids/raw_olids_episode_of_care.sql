{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.EPISODE_OF_CARE \ndbt: source(''olids'', ''EPISODE_OF_CARE'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  CARE_MANAGER_ORGANISATION_ID -> care_manager_organisation_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  EPISODE_TYPE_SOURCE_CONCEPT_ID -> episode_type_source_concept_id\n  EPISODE_TYPE_SOURCE_CODE -> episode_type_source_code\n  EPISODE_TYPE_SOURCE_DISPLAY -> episode_type_source_display\n  EPISODE_TYPE_CODE -> episode_type_code\n  EPISODE_TYPE_DISPLAY -> episode_type_display\n  EPISODE_STATUS_SOURCE_CONCEPT_ID -> episode_status_source_concept_id\n  EPISODE_STATUS_SOURCE_CODE -> episode_status_source_code\n  EPISODE_STATUS_SOURCE_DISPLAY -> episode_status_source_display\n  EPISODE_STATUS_CODE -> episode_status_code\n  EPISODE_STATUS_DISPLAY -> episode_status_display\n  EPISODE_OF_CARE_START_DATE -> episode_of_care_start_date\n  EPISODE_OF_CARE_END_DATE -> episode_of_care_end_date\n  CARE_MANAGER_PRACTITIONER_IN_ROLE_ID -> care_manager_practitioner_in_role_id\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  CARE_MANAGER_ORGANISATION_CODE -> care_manager_organisation_code\n  PATIENT_SHARD_ID -> patient_shard_id\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_FIRST_ACQUIRED -> lds_datetime_first_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "CARE_MANAGER_ORGANISATION_ID" as care_manager_organisation_id,
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
    "CARE_MANAGER_PRACTITIONER_IN_ROLE_ID" as care_manager_practitioner_in_role_id,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "CARE_MANAGER_ORGANISATION_CODE" as care_manager_organisation_code,
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
from {{ source('olids', 'EPISODE_OF_CARE') }}
