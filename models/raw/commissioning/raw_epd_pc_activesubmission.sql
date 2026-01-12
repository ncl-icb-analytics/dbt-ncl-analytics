{{
    config(
        description="Raw layer (Primary care medications and prescribing data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.EPD_PRIMARY_CARE.ActiveSubmission \ndbt: source(''epd_primary_care'', ''ActiveSubmission'') \nColumns:\n  UniqSubmissionId -> uniq_submission_id"
    )
}}
select
    "UniqSubmissionId" as uniq_submission_id
from {{ source('epd_primary_care', 'ActiveSubmission') }}
