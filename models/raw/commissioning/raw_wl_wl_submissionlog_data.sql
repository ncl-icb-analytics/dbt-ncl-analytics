-- Raw layer model for wl.WL_SubmissionLog_Data
-- Source: "DATA_LAKE"."WL"
-- Description: Waiting lists and patient pathway data
-- This is a 1:1 passthrough from source with standardized column names
select
    "derSubmissionID" as der_submission_id,
    "derDLPSpecificationName" as der_dlp_specification_name,
    "derFileName" as der_file_name,
    "derSubmissionDateTimeFromDLP" as der_submission_date_time_from_dlp,
    "derProviderCode" as der_provider_code,
    "derWeekEnding" as der_week_ending,
    "derIsLatestFiletypeProviderWeekending" as der_is_latest_filetype_provider_weekending,
    "derIsLatestFiletypeProvider" as der_is_latest_filetype_provider
from {{ source('wl', 'WL_SubmissionLog_Data') }}
