{{
    config(
        description="Raw layer (OLIDS stable layer - National Data Opt-Out preferences keyed by hashed NHS number). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.NDOO_HASHED \ndbt: source(''olids'', ''NDOO_HASHED'') \nColumns:\n  ID -> id\n  LDS_RECORD_ID -> lds_record_id\n  SK_PATIENT_ID -> sk_patient_id\n  NHS_NUMBER_HASH -> nhs_number_hash\n  PREFERENCE_TYPE -> preference_type\n  PREFERENCE_STATUS -> preference_status\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_BATCH_ID -> lds_batch_id\n  LDS_FILE_ID -> lds_file_id\n  LDS_DATASET_ID -> lds_dataset_id\n  EFFECTIVE_FROM -> effective_from\n  EFFECTIVE_TO -> effective_to\n  IS_LATEST -> is_latest\n  LAKEHOUSE_DATE_PROCESSED -> lakehouse_date_processed\n  HIGH_WATERMARK_DATE_TIME -> high_watermark_date_time"
    )
}}
select
    "ID" as id,
    "LDS_RECORD_ID" as lds_record_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "NHS_NUMBER_HASH" as nhs_number_hash,
    "PREFERENCE_TYPE" as preference_type,
    "PREFERENCE_STATUS" as preference_status,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_BATCH_ID" as lds_batch_id,
    "LDS_FILE_ID" as lds_file_id,
    "LDS_DATASET_ID" as lds_dataset_id,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_LATEST" as is_latest,
    "LAKEHOUSE_DATE_PROCESSED" as lakehouse_date_processed,
    "HIGH_WATERMARK_DATE_TIME" as high_watermark_date_time
from {{ source('olids', 'NDOO_HASHED') }}
