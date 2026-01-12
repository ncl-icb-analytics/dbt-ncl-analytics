{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Description \ndbt: source(''dictionary_snomed'', ''Description'') \nColumns:\n  SK_SnomedDescriptionID -> sk_snomed_description_id\n  SK_SnomedModuleID -> sk_snomed_module_id\n  SK_SnomedConceptID -> sk_snomed_concept_id\n  Term -> term\n  LanguageCode -> language_code\n  SK_SnomedTypeID -> sk_snomed_type_id\n  SK_SnomedCaseSignificanceID -> sk_snomed_case_significance_id\n  SK_SnomedRefSetID -> sk_snomed_ref_set_id\n  SK_SnomedAcceptabilityID -> sk_snomed_acceptability_id"
    )
}}
select
    "SK_SnomedDescriptionID" as sk_snomed_description_id,
    "SK_SnomedModuleID" as sk_snomed_module_id,
    "SK_SnomedConceptID" as sk_snomed_concept_id,
    "Term" as term,
    "LanguageCode" as language_code,
    "SK_SnomedTypeID" as sk_snomed_type_id,
    "SK_SnomedCaseSignificanceID" as sk_snomed_case_significance_id,
    "SK_SnomedRefSetID" as sk_snomed_ref_set_id,
    "SK_SnomedAcceptabilityID" as sk_snomed_acceptability_id
from {{ source('dictionary_snomed', 'Description') }}
