{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.INGESTION_ERROR_LOG \ndbt: source(''reference_fingertips'', ''INGESTION_ERROR_LOG'') \nColumns:\n  INDICATOR_ID -> indicator_id\n  AREA_ID -> area_id\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "INDICATOR_ID" as indicator_id,
    "AREA_ID" as area_id,
    "_TIMESTAMP" as timestamp
from {{ source('reference_fingertips', 'INGESTION_ERROR_LOG') }}
