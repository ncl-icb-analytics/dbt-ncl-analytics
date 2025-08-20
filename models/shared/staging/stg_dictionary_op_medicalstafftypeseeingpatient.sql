-- Staging model for dictionary_op.MedicalStaffTypeSeeingPatient
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_MedicalStaffTypeID" as sk_medicalstafftypeid,
    "BK_MedicalStaffTypeID" as bk_medicalstafftypeid,
    "MedicalStaffTypeSeeingPatient" as medicalstafftypeseeingpatient
from {{ source('dictionary_op', 'MedicalStaffTypeSeeingPatient') }}
