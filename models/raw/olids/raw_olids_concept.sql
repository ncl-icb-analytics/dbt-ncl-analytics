{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT \ndbt: source(''olids'', ''CONCEPT'') \nColumns:\n  ID -> id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  SYSTEM -> system\n  CODE -> code\n  DISPLAY -> display\n  IS_MAPPED -> is_mapped\n  USE_COUNT -> use_count\n  LDS_START_DATE_TIME -> lds_start_date_time"
    )
}}
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
from {{ source('olids', 'CONCEPT') }}
