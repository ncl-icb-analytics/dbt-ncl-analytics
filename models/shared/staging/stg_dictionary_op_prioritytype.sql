-- Staging model for dictionary_op.PriorityType
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_PriorityTypeID" as sk_prioritytypeid,
    "BK_PriorityTypeCode" as bk_prioritytypecode,
    "PriorityTypeDesc" as prioritytypedesc,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_op', 'PriorityType') }}
