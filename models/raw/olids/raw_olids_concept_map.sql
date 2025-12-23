-- Raw layer model for olids.CONCEPT_MAP
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "CONCEPT_MAP_ID" as concept_map_id,
    "CONCEPT_MAP_RESOURCE_ID" as concept_map_resource_id,
    "CONCEPT_MAP_URL" as concept_map_url,
    "CONCEPT_MAP_VERSION" as concept_map_version,
    "SOURCE_CODE_ID" as source_code_id,
    "SOURCE_SYSTEM" as source_system,
    "SOURCE_CODE" as source_code,
    "SOURCE_DISPLAY" as source_display,
    "TARGET_CODE_ID" as target_code_id,
    "TARGET_SYSTEM" as target_system,
    "TARGET_CODE" as target_code,
    "TARGET_DISPLAY" as target_display,
    "IS_PRIMARY" as is_primary,
    "IS_ACTIVE" as is_active,
    "EQUIVALENCE" as equivalence,
    "LDS_START_DATE_TIME" as lds_start_date_time
from {{ source('olids', 'CONCEPT_MAP') }}
