{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Specialties \ndbt: source(''dictionary_dbo'', ''Specialties'') \nColumns:\n  SK_SpecialtyID -> sk_specialty_id\n  BK_SpecialtyCode -> bk_specialty_code\n  SpecialtyName -> specialty_name\n  SpecialtyCategory -> specialty_category\n  IsTreatmentFunction -> is_treatment_function\n  IsMainSpecialty -> is_main_specialty\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  MainSpecialtyDescription -> main_specialty_description\n  TreatmentFunctionDescription -> treatment_function_description"
    )
}}
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
