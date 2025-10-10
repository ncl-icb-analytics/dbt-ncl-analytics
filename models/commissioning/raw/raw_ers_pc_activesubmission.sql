-- Raw layer model for eRS_primary_care.ActiveSubmission
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data
-- This is a 1:1 passthrough from source with standardized column names
select
    "UniqSubmissionID" as uniq_submission_id
from {{ source('eRS_primary_care', 'ActiveSubmission') }}
