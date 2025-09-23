-- Staging model for olids.PATIENT_PERSON
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records

select
    "LAKEHOUSEDATEPROCESSED" as lakehousedateprocessed,
    "LAKEHOUSEDATETIMEUPDATED" as lakehousedatetimeupdated,
    "LDS_RECORD_ID" as lds_record_id,
    "LDS_ID" as lds_id,
    "ID" as id,
    "LDS_DATETIME_DATA_ACQUIRED" as lds_datetime_data_acquired,
    "LDS_START_DATE_TIME" as lds_start_date_time,
    "LDS_DATASET_ID" as lds_dataset_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id
from {{ source('olids', 'PATIENT_PERSON') }}
