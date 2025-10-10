-- Raw layer model for dictionary_ip.AdmissionType
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_AdmissionTypeID" as sk_admission_type_id,
    "admission_type" as admission_type,
    "admission_type_group" as admission_type_group,
    "admission_type_description" as admission_type_description
from {{ source('dictionary_ip', 'AdmissionType') }}
