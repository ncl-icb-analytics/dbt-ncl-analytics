-- Staging model for dictionary_ecds.Comorbidity
-- Source: "Dictionary"."ECDS_ETOS"
-- Description: Reference data for ECDS

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
from {{ source('dictionary_ecds', 'Comorbidity') }}
