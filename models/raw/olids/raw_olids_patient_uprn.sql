{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_UPRN \ndbt: source(''olids'', ''PATIENT_UPRN'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  PATIENT_ADDRESS_ID -> patient_address_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  MASKED_UPRN -> masked_uprn\n  MASKED_USRN -> masked_usrn\n  MASKED_POSTCODE -> masked_postcode\n  ADDRESS_FORMAT_QUALITY -> address_format_quality\n  POSTCODE_QUALITY -> postcode_quality\n  MATCHED_WITH_ASSIGN -> matched_with_assign\n  QUALIFIER -> qualifier\n  CLASSIFICATION -> classification\n  ALGORITHM -> algorithm\n  MATCH_PATTERN -> match_pattern\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  PATIENT_SHARD_ID -> patient_shard_id\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_REGISTRAR_EVENT_ID -> lds_registrar_event_id\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PATIENT_ADDRESS_ID" as patient_address_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "MASKED_UPRN" as masked_uprn,
    "MASKED_USRN" as masked_usrn,
    "MASKED_POSTCODE" as masked_postcode,
    "ADDRESS_FORMAT_QUALITY" as address_format_quality,
    "POSTCODE_QUALITY" as postcode_quality,
    "MATCHED_WITH_ASSIGN" as matched_with_assign,
    "QUALIFIER" as qualifier,
    "CLASSIFICATION" as classification,
    "ALGORITHM" as algorithm,
    "MATCH_PATTERN" as match_pattern,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "PATIENT_SHARD_ID" as patient_shard_id,
    "PERSON_SHARD_ID" as person_shard_id,
    "LDS_SOURCE_RECORD_SHARD_ID" as lds_source_record_shard_id,
    "LDS_ID" as lds_id,
    "LDS_SOURCE_DATASET_ID" as lds_source_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_REGISTRAR_EVENT_ID" as lds_registrar_event_id,
    "LDS_DATETIME_UPDATE_ACQUIRED" as lds_datetime_update_acquired,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT_UPRN') }}
