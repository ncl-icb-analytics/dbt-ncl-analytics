-- Staging model for eRS_primary_care.ActiveSubmission
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data

select
    "UniqSubmissionID" as uniq_submission_id
from {{ source('eRS_primary_care', 'ActiveSubmission') }}
