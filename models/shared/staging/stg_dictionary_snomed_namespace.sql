-- Staging model for dictionary_snomed.Namespace
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "SK_NamespaceID" as sk_namespaceid,
    "DateIssued" as dateissued,
    "Organisation" as organisation,
    "Notes" as notes
from {{ source('dictionary_snomed', 'Namespace') }}
