{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.INDICATOR_UPDATE_LOG \ndbt: source(''reference_fingertips'', ''INDICATOR_UPDATE_LOG'') \nColumns:\n  INDICATOR_ID -> indicator_id\n  AREA_ID -> area_id\n  AREA_TYPE -> area_type\n  DATE_UPDATED_LOCAL -> date_updated_local\n  IS_LATEST -> is_latest\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "AREA_TYPE" as area_type,
    "DATE_UPDATED_LOCAL" as date_updated_local,
    "IS_LATEST" as is_latest,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INDICATOR_UPDATE_LOG') }}
