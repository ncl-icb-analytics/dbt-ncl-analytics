-- Raw layer model for olids.TERMINOLOGY_CONCEPT_MAP
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "CONCEPT_MAP_ID" as concept_map_id,
    "SOURCE_CODE_ID" as source_code_id,
    "TARGET_CODE_ID" as target_code_id,
    "IS_PRIMARY" as is_primary,
    "EQUIVALENCE" as equivalence,
    "LDS_START_DATE_TIME" as lds_start_date_time
from {{ source('olids', 'TERMINOLOGY_CONCEPT_MAP') }}
