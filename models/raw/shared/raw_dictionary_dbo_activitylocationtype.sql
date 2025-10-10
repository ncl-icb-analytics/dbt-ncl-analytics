-- Raw layer model for dictionary_dbo.ActivityLocationType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ActivityLocationTypeID" as sk_activity_location_type_id,
    "BK_ActivityLocationTypeCode" as bk_activity_location_type_code,
    "ActivityLocationTypeCategory" as activity_location_type_category,
    "ActivityLocationTypeDescription" as activity_location_type_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ActivityLocationType') }}
