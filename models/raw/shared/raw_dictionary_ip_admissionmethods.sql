{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.AdmissionMethods \ndbt: source(''dictionary_ip'', ''AdmissionMethods'') \nColumns:\n  SK_AdmissionMethodID -> sk_admission_method_id\n  BK_AdmissionMethodCode -> bk_admission_method_code\n  AdmissionMethodName -> admission_method_name\n  AdmissionMethodGroup -> admission_method_group\n  AdmissionMethodMethodFullName -> admission_method_method_full_name\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_AdmissionMethodID" as sk_admission_method_id,
    "BK_AdmissionMethodCode" as bk_admission_method_code,
    "AdmissionMethodName" as admission_method_name,
    "AdmissionMethodGroup" as admission_method_group,
    "AdmissionMethodMethodFullName" as admission_method_method_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_ip', 'AdmissionMethods') }}
