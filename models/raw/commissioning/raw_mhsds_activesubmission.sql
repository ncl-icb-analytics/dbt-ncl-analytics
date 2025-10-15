-- Raw layer model for mhsds.ActiveSubmission
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "OrgIDProvider" as org_id_provider,
    "UniqSubmissionID" as uniq_submission_id,
    "ReportingPeriodEndDate" as reporting_period_end_date
from {{ source('mhsds', 'ActiveSubmission') }}
