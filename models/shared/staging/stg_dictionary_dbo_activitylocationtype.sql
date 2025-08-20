-- Staging model for dictionary_dbo.ActivityLocationType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ActivityLocationTypeID" as sk_activitylocationtypeid,
    "BK_ActivityLocationTypeCode" as bk_activitylocationtypecode,
    "ActivityLocationTypeCategory" as activitylocationtypecategory,
    "ActivityLocationTypeDescription" as activitylocationtypedescription,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_dbo', 'ActivityLocationType') }}
