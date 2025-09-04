-- Staging model for dictionary_op.MedicalStaffTypeSeeingPatient
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_MedicalStaffTypeID" as sk_medical_staff_type_id,
    "BK_MedicalStaffTypeID" as bk_medical_staff_type_id,
    "MedicalStaffTypeSeeingPatient" as medical_staff_type_seeing_patient
from {{ source('dictionary_op', 'MedicalStaffTypeSeeingPatient') }}
