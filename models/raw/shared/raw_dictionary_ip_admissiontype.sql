{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.AdmissionType \ndbt: source(''dictionary_ip'', ''AdmissionType'') \nColumns:\n  SK_AdmissionTypeID -> sk_admission_type_id\n  admission_type -> admission_type\n  admission_type_group -> admission_type_group\n  admission_type_description -> admission_type_description"
    )
}}
select
    "SK_AdmissionTypeID" as sk_admission_type_id,
    "admission_type" as admission_type,
    "admission_type_group" as admission_type_group,
    "admission_type_description" as admission_type_description
from {{ source('dictionary_ip', 'AdmissionType') }}
