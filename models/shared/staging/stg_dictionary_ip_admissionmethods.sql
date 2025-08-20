-- Staging model for dictionary_ip.AdmissionMethods
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_AdmissionMethodID" as sk_admissionmethodid,
    "BK_AdmissionMethodCode" as bk_admissionmethodcode,
    "AdmissionMethodName" as admissionmethodname,
    "AdmissionMethodGroup" as admissionmethodgroup,
    "AdmissionMethodMethodFullName" as admissionmethodmethodfullname,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_ip', 'AdmissionMethods') }}
