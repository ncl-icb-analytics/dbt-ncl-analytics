{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.LOCATION \ndbt: source(''olids'', ''LOCATION'') \nColumns:\n  LDS_RECORD_ID -> lds_record_id\n  ID -> id\n  NAME -> name\n  TYPE_CODE -> type_code\n  TYPE_DESC -> type_desc\n  IS_PRIMARY_LOCATION -> is_primary_location\n  HOUSE_NAME -> house_name\n  HOUSE_NUMBER -> house_number\n  HOUSE_NAME_FLAT_NUMBER -> house_name_flat_number\n  STREET -> street\n  ADDRESS_LINE_1 -> address_line_1\n  ADDRESS_LINE_2 -> address_line_2\n  ADDRESS_LINE_3 -> address_line_3\n  ADDRESS_LINE_4 -> address_line_4\n  POSTCODE -> postcode\n  MANAGING_ORGANISATION_ID -> managing_organisation_id\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  IS_OBSOLETE -> is_obsolete\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  RECORD_OWNER_ORGANISATION_CODE -> record_owner_organisation_code\n  LDS_DATETIME_DATA_ACQUIRED -> lds_datetime_data_acquired\n  LDS_INITIAL_DATA_RECEIVED_DATE -> lds_initial_data_received_date\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATE_TIME -> lds_start_date_time\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "NAME" as name,
    "TYPE_CODE" as type_code,
    "TYPE_DESC" as type_desc,
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
from {{ source('olids', 'LOCATION') }}
