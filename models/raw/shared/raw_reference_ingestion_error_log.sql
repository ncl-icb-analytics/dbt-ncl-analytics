-- Raw layer model for reference_fingertips.INGESTION_ERROR_LOG
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INGESTION_ERROR_LOG') }}
