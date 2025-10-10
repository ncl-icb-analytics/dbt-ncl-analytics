-- Raw layer model for olids.TERMINOLOGY_CONCEPT
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
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
from {{ source('olids', 'TERMINOLOGY_CONCEPT') }}
