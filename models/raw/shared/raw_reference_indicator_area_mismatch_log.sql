-- Raw layer model for reference_fingertips.INDICATOR_AREA_MISMATCH_LOG
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "AREA_TYPE_DATA" as area_type_data,
    "AREA_TYPE_META" as area_type_meta
from {{ source('reference_fingertips', 'INDICATOR_AREA_MISMATCH_LOG') }}
