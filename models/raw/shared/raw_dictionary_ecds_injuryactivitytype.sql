{{
    config(
        description="Raw layer (Reference data for ECDS). 1:1 passthrough with cleaned column names. \nSource: Dictionary.ECDS_ETOS.InjuryActivityType \ndbt: source(''dictionary_ecds'', ''InjuryActivityType'') \nColumns:\n  ECDS_UniqueID -> ecds_unique_id\n  REFSET_UniqueID -> refset_unique_id\n  SNOMED_Code -> snomed_code\n  SNOMED_UK_Preferred_Term -> snomed_uk_preferred_term\n  SNOMED_Fully_Specified_Name -> snomed_fully_specified_name\n  ECDS_Description -> ecds_description\n  ECDS_Group1 -> ecds_group1\n  Sort1 -> sort1\n  Sort2 -> sort2\n  Sort3 -> sort3\n  Sort4 -> sort4\n  Notes -> notes\n  Valid_From -> valid_from\n  Valid_To -> valid_to\n  dv_IsActive -> dv_is_active"
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
    "Sort1" as sort1,
    "Sort2" as sort2,
    "Sort3" as sort3,
    "Sort4" as sort4,
    "Notes" as notes,
    "Valid_From" as valid_from,
    "Valid_To" as valid_to,
    "dv_IsActive" as dv_is_active
from {{ source('dictionary_ecds', 'InjuryActivityType') }}
