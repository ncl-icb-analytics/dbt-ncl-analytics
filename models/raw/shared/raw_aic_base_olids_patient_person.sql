-- Raw layer model for aic.BASE_OLIDS__PATIENT_PERSON
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
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
from {{ source('aic', 'BASE_OLIDS__PATIENT_PERSON') }}
