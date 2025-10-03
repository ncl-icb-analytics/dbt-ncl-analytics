-- Raw layer model for epd_primary_care.ActiveSubmission
-- Source: "DATA_LAKE"."EPD_PRIMARY_CARE"
-- Description: Primary care medications and prescribing data
-- This is a 1:1 passthrough from source with standardized column names
select
    "UniqSubmissionId" as uniq_submission_id
from {{ source('epd_primary_care', 'ActiveSubmission') }}
