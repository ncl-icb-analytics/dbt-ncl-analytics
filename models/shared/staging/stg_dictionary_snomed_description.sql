-- Staging model for dictionary_snomed.Description
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "SK_SnomedDescriptionID" as sk_snomeddescriptionid,
    "SK_SnomedModuleID" as sk_snomedmoduleid,
    "SK_SnomedConceptID" as sk_snomedconceptid,
    "Term" as term,
    "LanguageCode" as languagecode,
    "SK_SnomedTypeID" as sk_snomedtypeid,
    "SK_SnomedCaseSignificanceID" as sk_snomedcasesignificanceid,
    "SK_SnomedRefSetID" as sk_snomedrefsetid,
    "SK_SnomedAcceptabilityID" as sk_snomedacceptabilityid
from {{ source('dictionary_snomed', 'Description') }}
