-- Raw layer model for dictionary_ip.SourceOfAdmissions
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_SourceOfAdmissionID" as sk_source_of_admission_id,
    "BK_SourceOfAdmissionCode" as bk_source_of_admission_code,
    "SourceOfAdmissionName" as source_of_admission_name,
    "SourceOfAdmissionFullName" as source_of_admission_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_ip', 'SourceOfAdmissions') }}
