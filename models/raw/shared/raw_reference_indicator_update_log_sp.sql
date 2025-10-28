-- Raw layer model for reference_fingertips.INDICATOR_UPDATE_LOG_SP
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "DATE_UPDATED_LOCAL" as date_updated_local,
    "IS_LATEST" as is_latest,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INDICATOR_UPDATE_LOG_SP') }}
