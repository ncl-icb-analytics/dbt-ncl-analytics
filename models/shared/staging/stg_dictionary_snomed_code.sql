-- Staging model for dictionary_snomed.Code
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "SK_SnomedID" as sk_snomedid,
    "SnomedItemID" as snomeditemid,
    "SnomedNamespaceID" as snomednamespaceid,
    "SnomedPartitionID" as snomedpartitionid,
    "SnomedCheckDigit" as snomedcheckdigit,
    "InNationalDataset" as innationaldataset,
    "SnomedCodeType" as snomedcodetype,
    "SK_SnomedConceptID" as sk_snomedconceptid,
    "SK_SnomedDescriptionID" as sk_snomeddescriptionid,
    "IsSensitive" as issensitive
from {{ source('dictionary_snomed', 'Code') }}
