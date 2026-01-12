{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.DNAIndicators \ndbt: source(''dictionary_op'', ''DNAIndicators'') \nColumns:\n  SK_DNAIndicatorID -> sk_dna_indicator_id\n  BK_DNACode -> bk_dna_code\n  DNAIndicatorDesc -> dna_indicator_desc\n  DNAIndicatorStatus -> dna_indicator_status\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_DNAIndicatorID" as sk_dna_indicator_id,
    "BK_DNACode" as bk_dna_code,
    "DNAIndicatorDesc" as dna_indicator_desc,
    "DNAIndicatorStatus" as dna_indicator_status,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'DNAIndicators') }}
