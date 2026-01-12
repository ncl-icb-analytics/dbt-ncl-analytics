{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.INDICATOR_AREA_MISMATCH_LOG \ndbt: source(''reference_fingertips'', ''INDICATOR_AREA_MISMATCH_LOG'') \nColumns:\n  INDICATOR_ID -> indicator_id\n  AREA_ID -> area_id\n  AREA_TYPE_DATA -> area_type_data\n  AREA_TYPE_META -> area_type_meta"
    )
}}
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "AREA_TYPE_DATA" as area_type_data,
    "AREA_TYPE_META" as area_type_meta
from {{ source('reference_fingertips', 'INDICATOR_AREA_MISMATCH_LOG') }}
