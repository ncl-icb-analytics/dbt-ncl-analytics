-- Raw layer model for csds.ActiveSubmission
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "UNIQUE SUBMISSION ID" as unique_submission_id
from {{ source('csds', 'ActiveSubmission') }}
