{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.SourceOfAdmissions \ndbt: source(''dictionary_ip'', ''SourceOfAdmissions'') \nColumns:\n  SK_SourceOfAdmissionID -> sk_source_of_admission_id\n  BK_SourceOfAdmissionCode -> bk_source_of_admission_code\n  SourceOfAdmissionName -> source_of_admission_name\n  SourceOfAdmissionFullName -> source_of_admission_full_name\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_SourceOfAdmissionID" as sk_source_of_admission_id,
    "BK_SourceOfAdmissionCode" as bk_source_of_admission_code,
    "SourceOfAdmissionName" as source_of_admission_name,
    "SourceOfAdmissionFullName" as source_of_admission_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_ip', 'SourceOfAdmissions') }}
