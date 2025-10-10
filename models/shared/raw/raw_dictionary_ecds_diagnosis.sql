-- Raw layer model for dictionary_ecds.Diagnosis
-- Source: "Dictionary"."ECDS_ETOS"
-- Description: Reference data for ECDS
-- This is a 1:1 passthrough from source with standardized column names
select
    "ECDS_UniqueID" as ecds_unique_id,
    "REFSET_UniqueID" as refset_unique_id,
    "SNOMED_Code" as snomed_code,
    "SNOMED_UK_Preferred_Term" as snomed_uk_preferred_term,
    "SNOMED_Fully_Specified_Name" as snomed_fully_specified_name,
    "ECDS_Description" as ecds_description,
    "ECDS_Group1" as ecds_group1,
    "ECDS_Group2" as ecds_group2,
    "ECDS_Group3" as ecds_group3,
    "Flag_Allergy" as flag_allergy,
    "Flag_NotifiableDisease" as flag_notifiable_disease,
    "Flag_Injury" as flag_injury,
    "Flag_Male" as flag_male,
    "Flag_Female" as flag_female,
    "Flag_SDEC" as flag_sdec,
    "Flag_ADS" as flag_ads,
    "ICD10_Mapping" as icd10_mapping,
    "ICD10_Description" as icd10_description,
    "ICD11_Mapping" as icd11_mapping,
    "ICD11_Description" as icd11_description,
    "Sort1" as sort1,
    "Sort2" as sort2,
    "Sort3" as sort3,
    "Sort4" as sort4,
    "Notes" as notes,
    "Valid_From" as valid_from,
    "Valid_To" as valid_to,
    "dv_IsActive" as dv_is_active
from {{ source('dictionary_ecds', 'Diagnosis') }}
