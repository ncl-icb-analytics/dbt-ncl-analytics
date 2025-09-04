-- Staging model for dictionary_dbo.PatientClassification
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PatientClassificationID" as sk_patient_classification_id,
    "BK_PatientClassificationCode" as bk_patient_classification_code,
    "PatientClassificationName" as patient_classification_name,
    "PatientClassificationFullName" as patient_classification_full_name
from {{ source('dictionary_dbo', 'PatientClassification') }}
