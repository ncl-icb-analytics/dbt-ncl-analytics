-- Staging model for dictionary_op.DNAIndicators
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_DNAIndicatorID" as sk_dnaindicatorid,
    "BK_DNACode" as bk_dnacode,
    "DNAIndicatorDesc" as dnaindicatordesc,
    "DNAIndicatorStatus" as dnaindicatorstatus,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_op', 'DNAIndicators') }}
