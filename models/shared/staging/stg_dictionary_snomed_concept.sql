-- Staging model for dictionary_snomed.Concept
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "SK_SnomedConceptID" as sk_snomedconceptid,
    "SK_SnomedModuleID" as sk_snomedmoduleid,
    "SK_SnomedDescriptionID" as sk_snomeddescriptionid,
    "PreferredTerm" as preferredterm,
    "SK_SnomedDefinitionStatusID" as sk_snomeddefinitionstatusid,
    "DefinitionStatus" as definitionstatus,
    "IsActive" as isactive,
    "InNationalDataset" as innationaldataset,
    "LastUpdated" as lastupdated
from {{ source('dictionary_snomed', 'Concept') }}
