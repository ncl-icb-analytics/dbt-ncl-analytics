-- Staging model for csds.ActiveSubmission
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

select
    "UNIQUE SUBMISSION ID" as unique_submission_id
from {{ source('csds', 'ActiveSubmission') }}
