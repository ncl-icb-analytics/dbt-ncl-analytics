-- Staging model for olids.LOCATION_CONTACT
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "LOCATION_ID" as location_id,
    "IS_PRIMARY_CONTACT" as is_primary_contact,
    "CONTACT_TYPE" as contact_type,
    "CONTACT_TYPE_CONCEPT_ID" as contact_type_concept_id,
    "VALUE" as value,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'LOCATION_CONTACT') }}
