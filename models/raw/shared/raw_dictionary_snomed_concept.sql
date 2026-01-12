{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Concept \ndbt: source(''dictionary_snomed'', ''Concept'') \nColumns:\n  SK_SnomedConceptID -> sk_snomed_concept_id\n  SK_SnomedModuleID -> sk_snomed_module_id\n  SK_SnomedDescriptionID -> sk_snomed_description_id\n  PreferredTerm -> preferred_term\n  SK_SnomedDefinitionStatusID -> sk_snomed_definition_status_id\n  DefinitionStatus -> definition_status\n  IsActive -> is_active\n  InNationalDataset -> in_national_dataset\n  LastUpdated -> last_updated"
    )
}}
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
