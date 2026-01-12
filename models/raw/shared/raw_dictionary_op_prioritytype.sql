{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.PriorityType \ndbt: source(''dictionary_op'', ''PriorityType'') \nColumns:\n  SK_PriorityTypeID -> sk_priority_type_id\n  BK_PriorityTypeCode -> bk_priority_type_code\n  PriorityTypeDesc -> priority_type_desc\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_PriorityTypeID" as sk_priority_type_id,
    "BK_PriorityTypeCode" as bk_priority_type_code,
    "PriorityTypeDesc" as priority_type_desc,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'PriorityType') }}
