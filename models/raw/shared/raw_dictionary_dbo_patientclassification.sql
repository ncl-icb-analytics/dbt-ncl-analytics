{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PatientClassification \ndbt: source(''dictionary_dbo'', ''PatientClassification'') \nColumns:\n  SK_PatientClassificationID -> sk_patient_classification_id\n  BK_PatientClassificationCode -> bk_patient_classification_code\n  PatientClassificationName -> patient_classification_name\n  PatientClassificationFullName -> patient_classification_full_name"
    )
}}
select
    "SK_PatientClassificationID" as sk_patient_classification_id,
    "BK_PatientClassificationCode" as bk_patient_classification_code,
    "PatientClassificationName" as patient_classification_name,
    "PatientClassificationFullName" as patient_classification_full_name
from {{ source('dictionary_dbo', 'PatientClassification') }}
