-- Staging model for reference_fingertips.METADATA_AREA
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data

select
    "AREA_ID" as area_id,
    "Name" as name,
    "Short" as short,
    "Class" as class,
    "Sequence" as sequence,
    "CanBeDisplayedOnMap" as can_be_displayed_on_map
from {{ source('reference_fingertips', 'METADATA_AREA') }}
