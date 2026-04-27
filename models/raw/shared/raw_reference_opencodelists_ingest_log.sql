{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.OPENCODELISTS_INGEST_LOG \ndbt: source(''reference_terminology'', ''OPENCODELISTS_INGEST_LOG'') \nColumns:\n  RUN_ID -> run_id\n  RUN_START -> run_start\n  RUN_END -> run_end\n  FULL_REBUILD -> full_rebuild\n  STATUS -> status\n  CODELISTS_AVAILABLE -> codelists_available\n  CODELISTS_LOADED -> codelists_loaded\n  CODES_LOADED -> codes_loaded\n  REQUESTS_MADE -> requests_made\n  FAILED_REQUESTS -> failed_requests\n  DURATION_SECONDS -> duration_seconds\n  ERROR_MESSAGE -> error_message"
    )
}}
select
    "RUN_ID" as run_id,
    "RUN_START" as run_start,
    "RUN_END" as run_end,
    "FULL_REBUILD" as full_rebuild,
    "STATUS" as status,
    "CODELISTS_AVAILABLE" as codelists_available,
    "CODELISTS_LOADED" as codelists_loaded,
    "CODES_LOADED" as codes_loaded,
    "REQUESTS_MADE" as requests_made,
    "FAILED_REQUESTS" as failed_requests,
    "DURATION_SECONDS" as duration_seconds,
    "ERROR_MESSAGE" as error_message
from {{ source('reference_terminology', 'OPENCODELISTS_INGEST_LOG') }}
