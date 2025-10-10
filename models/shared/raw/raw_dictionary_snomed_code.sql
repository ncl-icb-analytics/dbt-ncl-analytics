-- Raw layer model for dictionary_snomed.Code
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_SnomedID" as sk_snomed_id,
    "SnomedItemID" as snomed_item_id,
    "SnomedNamespaceID" as snomed_namespace_id,
    "SnomedPartitionID" as snomed_partition_id,
    "SnomedCheckDigit" as snomed_check_digit,
    "InNationalDataset" as in_national_dataset,
    "SnomedCodeType" as snomed_code_type,
    "SK_SnomedConceptID" as sk_snomed_concept_id,
    "SK_SnomedDescriptionID" as sk_snomed_description_id,
    "IsSensitive" as is_sensitive
from {{ source('dictionary_snomed', 'Code') }}
