-- Raw layer model for reference_fingertips.INDICATOR_UPDATE_LOG
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "AREA_TYPE" as area_type,
    "DATE_UPDATED_LOCAL" as date_updated_local,
    "IS_LATEST" as is_latest,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INDICATOR_UPDATE_LOG') }}
