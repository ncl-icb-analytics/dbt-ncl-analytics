{{
    config(
        description="Raw layer (Waiting lists and patient pathway data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.WL.WL_SubmissionLog_Data \ndbt: source(''wl'', ''WL_SubmissionLog_Data'') \nColumns:\n  derSubmissionID -> der_submission_id\n  derDLPSpecificationName -> der_dlp_specification_name\n  derFileName -> der_file_name\n  derSubmissionDateTimeFromDLP -> der_submission_date_time_from_dlp\n  derProviderCode -> der_provider_code\n  derWeekEnding -> der_week_ending\n  derIsLatestFiletypeProviderWeekending -> der_is_latest_filetype_provider_weekending\n  derIsLatestFiletypeProvider -> der_is_latest_filetype_provider"
    )
}}
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
