{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.MedicalStaffTypeSeeingPatient \ndbt: source(''dictionary_op'', ''MedicalStaffTypeSeeingPatient'') \nColumns:\n  SK_MedicalStaffTypeID -> sk_medical_staff_type_id\n  BK_MedicalStaffTypeID -> bk_medical_staff_type_id\n  MedicalStaffTypeSeeingPatient -> medical_staff_type_seeing_patient"
    )
}}
select
    "SK_MedicalStaffTypeID" as sk_medical_staff_type_id,
    "BK_MedicalStaffTypeID" as bk_medical_staff_type_id,
    "MedicalStaffTypeSeeingPatient" as medical_staff_type_seeing_patient
from {{ source('dictionary_op', 'MedicalStaffTypeSeeingPatient') }}
