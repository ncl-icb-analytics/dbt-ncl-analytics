-- Raw layer model for dictionary_snomed.Description
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed
-- This is a 1:1 passthrough from source with standardized column names
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
