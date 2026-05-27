{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PRACTITIONER \ndbt: source(''olids'', ''PRACTITIONER'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  GMC_CODE -> gmc_code\n  TITLE -> title\n  FIRST_NAME -> first_name\n  SURNAME -> surname\n  NAME -> name\n  IS_OBSOLETE -> is_obsolete\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_FIRST_ACQUIRED -> lds_datetime_first_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "GMC_CODE" as gmc_code,
    "TITLE" as title,
    "FIRST_NAME" as first_name,
    "SURNAME" as surname,
    "NAME" as name,
    "IS_OBSOLETE" as is_obsolete,
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
from {{ source('olids', 'PRACTITIONER') }}
