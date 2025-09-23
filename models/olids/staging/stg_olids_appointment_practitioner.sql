-- Staging model for olids.APPOINTMENT_PRACTITIONER
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "APPOINTMENT_ID" as appointment_id,
    "PRACTITIONER_ID" as practitioner_id,
    "LDS_END_DATE_TIME" as lds_end_date_time,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "LDS_RECORD_ID_USER" as lds_record_id_user,
    "RECORD_OWNER_ORGANISATION_CODE" as record_owner_organisation_code,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'APPOINTMENT_PRACTITIONER') }}
