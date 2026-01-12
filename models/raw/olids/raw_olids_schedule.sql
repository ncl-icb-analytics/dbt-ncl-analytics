{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.SCHEDULE \ndbt: source(''olids'', ''SCHEDULE'') \nColumns:\n  LDS_RECORD_ID -> lds_record_id\n  ID -> id\n  LOCATION_ID -> location_id\n  LOCATION -> location\n  PRACTITIONER_ID -> practitioner_id\n  START_DATE -> start_date\n  END_DATE -> end_date\n  TYPE -> type\n  NAME -> name\n  IS_PRIVATE -> is_private\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  RECORD_OWNER_ORGANISATION_CODE -> record_owner_organisation_code\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "LOCATION_ID" as location_id,
    "LOCATION" as location,
    "PRACTITIONER_ID" as practitioner_id,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "TYPE" as type,
    "NAME" as name,
    "IS_PRIVATE" as is_private,
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
from {{ source('olids', 'SCHEDULE') }}
