-- Staging model for dictionary_dbo.Urgent_Emergency_Care_Activity_Type
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_Urgent_Emergency_Care_Activity_Type_ID" as sk_urgent_emergency_care_activity_type_id,
    "BK_Urgent_Emergency_Care_Activity_Type_Code" as bk_urgent_emergency_care_activity_type_code,
    "Urgent_Emergency_Care_Activity_Type_Description" as urgent_emergency_care_activity_type_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Urgent_Emergency_Care_Activity_Type') }}
