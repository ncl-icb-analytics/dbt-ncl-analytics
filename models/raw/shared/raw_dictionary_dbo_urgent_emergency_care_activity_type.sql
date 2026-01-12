{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Urgent_Emergency_Care_Activity_Type \ndbt: source(''dictionary_dbo'', ''Urgent_Emergency_Care_Activity_Type'') \nColumns:\n  SK_Urgent_Emergency_Care_Activity_Type_ID -> sk_urgent_emergency_care_activity_type_id\n  BK_Urgent_Emergency_Care_Activity_Type_Code -> bk_urgent_emergency_care_activity_type_code\n  Urgent_Emergency_Care_Activity_Type_Description -> urgent_emergency_care_activity_type_description\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_Urgent_Emergency_Care_Activity_Type_ID" as sk_urgent_emergency_care_activity_type_id,
    "BK_Urgent_Emergency_Care_Activity_Type_Code" as bk_urgent_emergency_care_activity_type_code,
    "Urgent_Emergency_Care_Activity_Type_Description" as urgent_emergency_care_activity_type_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Urgent_Emergency_Care_Activity_Type') }}
