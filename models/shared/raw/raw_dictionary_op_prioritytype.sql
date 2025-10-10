-- Raw layer model for dictionary_op.PriorityType
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PriorityTypeID" as sk_priority_type_id,
    "BK_PriorityTypeCode" as bk_priority_type_code,
    "PriorityTypeDesc" as priority_type_desc,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'PriorityType') }}
