{{
    config(
        description="Raw layer (Reference data for ECDS). 1:1 passthrough with cleaned column names. \nSource: Dictionary.ECDS_ETOS.Diagnosis \ndbt: source(''dictionary_ecds'', ''Diagnosis'') \nColumns:\n  ECDS_UniqueID -> ecds_unique_id\n  REFSET_UniqueID -> refset_unique_id\n  SNOMED_Code -> snomed_code\n  SNOMED_UK_Preferred_Term -> snomed_uk_preferred_term\n  SNOMED_Fully_Specified_Name -> snomed_fully_specified_name\n  ECDS_Description -> ecds_description\n  ECDS_Group1 -> ecds_group1\n  ECDS_Group2 -> ecds_group2\n  ECDS_Group3 -> ecds_group3\n  Flag_Allergy -> flag_allergy\n  Flag_NotifiableDisease -> flag_notifiable_disease\n  Flag_Injury -> flag_injury\n  Flag_Male -> flag_male\n  Flag_Female -> flag_female\n  Flag_SDEC -> flag_sdec\n  Flag_ADS -> flag_ads\n  ICD10_Mapping -> icd10_mapping\n  ICD10_Description -> icd10_description\n  ICD11_Mapping -> icd11_mapping\n  ICD11_Description -> icd11_description\n  Sort1 -> sort1\n  Sort2 -> sort2\n  Sort3 -> sort3\n  Sort4 -> sort4\n  Notes -> notes\n  Valid_From -> valid_from\n  Valid_To -> valid_to\n  dv_IsActive -> dv_is_active"
    )
}}
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
