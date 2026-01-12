{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.METADATA_AREA \ndbt: source(''reference_fingertips'', ''METADATA_AREA'') \nColumns:\n  AREA_ID -> area_id\n  Name -> name\n  Short -> short\n  Class -> class\n  Sequence -> sequence\n  CanBeDisplayedOnMap -> can_be_displayed_on_map"
    )
}}
select
    "AREA_ID" as area_id,
    "Name" as name,
    "Short" as short,
    "Class" as class,
    "Sequence" as sequence,
    "CanBeDisplayedOnMap" as can_be_displayed_on_map
from {{ source('reference_fingertips', 'METADATA_AREA') }}
