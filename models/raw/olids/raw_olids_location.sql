{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.LOCATION \ndbt: source(''olids'', ''LOCATION'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  NAME -> name\n  LOCATION_TYPE_SOURCE_CONCEPT_ID -> location_type_source_concept_id\n  TYPE_DESCRIPTION -> type_description\n  IS_PRIMARY_LOCATION -> is_primary_location\n  HOUSE_NAME -> house_name\n  HOUSE_NUMBER -> house_number\n  HOUSE_NAME_FLAT_NUMBER -> house_name_flat_number\n  STREET -> street\n  ADDRESS_LINE_1 -> address_line_1\n  ADDRESS_LINE_2 -> address_line_2\n  ADDRESS_LINE_3 -> address_line_3\n  ADDRESS_LINE_4 -> address_line_4\n  POSTCODE -> postcode\n  MANAGING_ORGANISATION_ID -> managing_organisation_id\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  IS_OBSOLETE -> is_obsolete\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_FIRST_ACQUIRED -> lds_datetime_first_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "NAME" as name,
    "LOCATION_TYPE_SOURCE_CONCEPT_ID" as location_type_source_concept_id,
    "TYPE_DESCRIPTION" as type_description,
    "IS_PRIMARY_LOCATION" as is_primary_location,
    "HOUSE_NAME" as house_name,
    "HOUSE_NUMBER" as house_number,
    "HOUSE_NAME_FLAT_NUMBER" as house_name_flat_number,
    "STREET" as street,
    "ADDRESS_LINE_1" as address_line_1,
    "ADDRESS_LINE_2" as address_line_2,
    "ADDRESS_LINE_3" as address_line_3,
    "ADDRESS_LINE_4" as address_line_4,
    "POSTCODE" as postcode,
    "MANAGING_ORGANISATION_ID" as managing_organisation_id,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "IS_OBSOLETE" as is_obsolete,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
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
from {{ source('olids', 'LOCATION') }}
