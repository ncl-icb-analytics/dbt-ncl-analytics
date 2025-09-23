-- Staging model for olids.PATIENT_UPRN
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

select
    "LDS_RECORD_ID" as lds_record_id,
    "ID" as id,
    "REGISTRAR_EVENT_ID" as registrar_event_id,
    "MASKED_UPRN" as masked_uprn,
    "MASKED_USRN" as masked_usrn,
    "MASKED_POSTCODE" as masked_postcode,
    "ADDRESS_FORMAT_QUALITY" as address_format_quality,
    "POST_CODE_QUALITY" as post_code_quality,
    "MATCHED_WITH_ASSIGN" as matched_with_assign,
    "QUALIFIER" as qualifier,
    "UPRN_PROPERTY_CLASSIFICATION" as uprn_property_classification,
    "ALGORITHM" as algorithm,
    "MATCH_PATTERN" as match_pattern,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_REGISTRAR_EVENT_ID" as lds_registrar_event_id,
    "RECORD_OWNER_ORGANISATION_CODE" as record_owner_organisation_code,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_INITIAL_DATA_RECEIVED_DATE" as lds_initial_data_received_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PATIENT_UPRN') }}
