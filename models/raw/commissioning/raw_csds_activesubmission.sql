{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.ActiveSubmission \ndbt: source(''csds'', ''ActiveSubmission'') \nColumns:\n  UNIQUE SUBMISSION ID -> unique_submission_id"
    )
}}
select
    "UNIQUE SUBMISSION ID" as unique_submission_id
from {{ source('csds', 'ActiveSubmission') }}
