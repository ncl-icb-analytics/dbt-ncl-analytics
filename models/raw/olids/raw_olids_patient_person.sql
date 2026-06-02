{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_PERSON \ndbt: source(''olids'', ''PATIENT_PERSON'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  LDS_SOURCE_RECORD_ID_PERSON -> lds_source_record_id_person\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  PERSON_UUID -> person_uuid\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  PATIENT_SHARD_ID -> patient_shard_id\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_REGISTRAR_EVENT_ID -> lds_registrar_event_id\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED_PERSON -> lds_datetime_update_acquired_person\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_END_DATETIME -> lds_end_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "LDS_SOURCE_RECORD_ID_PERSON" as lds_source_record_id_person,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PERSON_UUID" as person_uuid,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "PATIENT_SHARD_ID" as patient_shard_id,
    "PERSON_SHARD_ID" as person_shard_id,
    "LDS_SOURCE_RECORD_SHARD_ID" as lds_source_record_shard_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_SOURCE_DATASET_ID" as lds_source_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_REGISTRAR_EVENT_ID" as lds_registrar_event_id,
    "LDS_DATETIME_UPDATE_ACQUIRED" as lds_datetime_update_acquired,
    "LDS_DATETIME_UPDATE_ACQUIRED_PERSON" as lds_datetime_update_acquired_person,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime,
    "LDS_END_DATETIME" as lds_end_datetime,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT_PERSON') }}
