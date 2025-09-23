-- Staging model for olids.PATIENT_ADDRESS
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "PATIENT_ID" as patient_id,
    "ADDRESS_TYPE_CONCEPT_ID" as address_type_concept_id,
    "POSTCODE_HASH" as postcode_hash,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "PERSON_ID" as person_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "RECORD_OWNER_ORGANISATION_CODE" as record_owner_organisation_code,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATE_RECEIVED_DATE" as lds_initial_date_received_date,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT_ADDRESS') }}
