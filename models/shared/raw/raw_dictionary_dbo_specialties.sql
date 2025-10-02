-- Raw layer model for dictionary_dbo.Specialties
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_SpecialtyID" as sk_specialty_id,
    "BK_SpecialtyCode" as bk_specialty_code,
    "SpecialtyName" as specialty_name,
    "SpecialtyCategory" as specialty_category,
    "IsTreatmentFunction" as is_treatment_function,
    "IsMainSpecialty" as is_main_specialty,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "MainSpecialtyDescription" as main_specialty_description,
    "TreatmentFunctionDescription" as treatment_function_description
from {{ source('dictionary_dbo', 'Specialties') }}
