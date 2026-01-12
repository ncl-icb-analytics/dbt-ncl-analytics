{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Code \ndbt: source(''dictionary_snomed'', ''Code'') \nColumns:\n  SK_SnomedID -> sk_snomed_id\n  SnomedItemID -> snomed_item_id\n  SnomedNamespaceID -> snomed_namespace_id\n  SnomedPartitionID -> snomed_partition_id\n  SnomedCheckDigit -> snomed_check_digit\n  InNationalDataset -> in_national_dataset\n  SnomedCodeType -> snomed_code_type\n  SK_SnomedConceptID -> sk_snomed_concept_id\n  SK_SnomedDescriptionID -> sk_snomed_description_id\n  IsSensitive -> is_sensitive"
    )
}}
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
