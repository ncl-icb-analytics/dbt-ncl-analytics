-- Staging model for dictionary.ActivityLocationType
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_ActivityLocationTypeID" as sk_activitylocationtypeid,
    "BK_ActivityLocationTypeCode" as bk_activitylocationtypecode,
    "ActivityLocationTypeCategory" as activitylocationtypecategory,
    "ActivityLocationTypeDescription" as activitylocationtypedescription,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ActivityLocationType') }}
