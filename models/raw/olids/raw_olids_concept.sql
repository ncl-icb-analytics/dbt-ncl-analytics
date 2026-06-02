{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT \ndbt: source(''olids'', ''CONCEPT'') \nColumns:\n  CONCEPT_ID -> concept_id\n  SYSTEM -> system\n  CODE -> code\n  DISPLAY -> display\n  IS_MAPPED -> is_mapped\n  USE_COUNT -> use_count\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime"
    )
}}
select
    "CONCEPT_ID" as concept_id,
    "SYSTEM" as system,
    "CODE" as code,
    "DISPLAY" as display,
    "IS_MAPPED" as is_mapped,
    "USE_COUNT" as use_count,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime
from {{ source('olids', 'CONCEPT') }}
