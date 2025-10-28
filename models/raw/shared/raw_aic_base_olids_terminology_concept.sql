-- Raw layer model for aic.BASE_OLIDS__TERMINOLOGY_CONCEPT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "SYSTEM" as system,
    "CODE" as code,
    "DISPLAY" as display,
    "IS_MAPPED" as is_mapped,
    "USE_COUNT" as use_count,
    "LDS_START_DATE_TIME" as lds_start_date_time
from {{ source('aic', 'BASE_OLIDS__TERMINOLOGY_CONCEPT') }}
