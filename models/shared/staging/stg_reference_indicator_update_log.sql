-- Staging model for reference_fingertips.INDICATOR_UPDATE_LOG
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data

select
    "INDICATOR_ID" as indicator_id,
    "DATE_UPDATED_LOCAL" as date_updated_local,
    "IS_LATEST" as is_latest,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INDICATOR_UPDATE_LOG') }}
