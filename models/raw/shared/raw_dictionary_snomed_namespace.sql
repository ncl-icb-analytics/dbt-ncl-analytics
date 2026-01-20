{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Namespace \ndbt: source(''dictionary_snomed'', ''Namespace'') \nColumns:\n  SK_NamespaceID -> sk_namespace_id\n  DateIssued -> date_issued\n  Organisation -> organisation\n  Notes -> notes"
    )
}}
select
    "SK_NamespaceID" as sk_namespace_id,
    "DateIssued" as date_issued,
    "Organisation" as organisation,
    "Notes" as notes
from {{ source('dictionary_snomed', 'Namespace') }}
