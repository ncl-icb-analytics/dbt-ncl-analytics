-- Raw layer model for dictionary_snomed.Concept
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_SnomedConceptID" as sk_snomed_concept_id,
    "SK_SnomedModuleID" as sk_snomed_module_id,
    "SK_SnomedDescriptionID" as sk_snomed_description_id,
    "PreferredTerm" as preferred_term,
    "SK_SnomedDefinitionStatusID" as sk_snomed_definition_status_id,
    "DefinitionStatus" as definition_status,
    "IsActive" as is_active,
    "InNationalDataset" as in_national_dataset,
    "LastUpdated" as last_updated
from {{ source('dictionary_snomed', 'Concept') }}
