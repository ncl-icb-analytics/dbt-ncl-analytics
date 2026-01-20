{{
    config(
        description="Raw layer (Primary care referrals data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.ERS.ActiveSubmission \ndbt: source(''eRS_primary_care'', ''ActiveSubmission'') \nColumns:\n  UniqSubmissionID -> uniq_submission_id"
    )
}}
select
    "UniqSubmissionID" as uniq_submission_id
from {{ source('eRS_primary_care', 'ActiveSubmission') }}
