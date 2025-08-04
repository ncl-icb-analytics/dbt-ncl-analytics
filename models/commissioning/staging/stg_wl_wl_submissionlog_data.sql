-- Staging model for wl.WL_SubmissionLog_Data
-- Source: "DATA_LAKE"."WL"
{% if source.get('description') %}
-- Description: Waiting lists and patient pathway data
{% endif %}

select
    "derSubmissionID" as dersubmissionid,
    "derDLPSpecificationName" as derdlpspecificationname,
    "derFileName" as derfilename,
    "derSubmissionDateTimeFromDLP" as dersubmissiondatetimefromdlp,
    "derProviderCode" as derprovidercode,
    "derWeekEnding" as derweekending,
    "derIsLatestFiletypeProviderWeekending" as derislatestfiletypeproviderweekending,
    "derIsLatestFiletypeProvider" as derislatestfiletypeprovider
from {{ source('wl', 'WL_SubmissionLog_Data') }}
