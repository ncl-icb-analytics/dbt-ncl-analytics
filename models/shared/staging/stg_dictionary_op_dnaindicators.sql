-- Staging model for dictionary_op.DNAIndicators
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_DNAIndicatorID" as sk_dna_indicator_id,
    "BK_DNACode" as bk_dna_code,
    "DNAIndicatorDesc" as dna_indicator_desc,
    "DNAIndicatorStatus" as dna_indicator_status,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'DNAIndicators') }}
