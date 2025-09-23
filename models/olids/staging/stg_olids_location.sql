-- Staging model for olids.LOCATION
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

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
