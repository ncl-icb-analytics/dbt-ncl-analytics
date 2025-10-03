-- Raw layer model for dictionary_snomed.Namespace
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_NamespaceID" as sk_namespace_id,
    "DateIssued" as date_issued,
    "Organisation" as organisation,
    "Notes" as notes
from {{ source('dictionary_snomed', 'Namespace') }}
