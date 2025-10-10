-- Raw layer model for dictionary_ip.AdmissionMethods
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_AdmissionMethodID" as sk_admission_method_id,
    "BK_AdmissionMethodCode" as bk_admission_method_code,
    "AdmissionMethodName" as admission_method_name,
    "AdmissionMethodGroup" as admission_method_group,
    "AdmissionMethodMethodFullName" as admission_method_method_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_ip', 'AdmissionMethods') }}
