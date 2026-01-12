{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ActivityLocationType \ndbt: source(''dictionary_dbo'', ''ActivityLocationType'') \nColumns:\n  SK_ActivityLocationTypeID -> sk_activity_location_type_id\n  BK_ActivityLocationTypeCode -> bk_activity_location_type_code\n  ActivityLocationTypeCategory -> activity_location_type_category\n  ActivityLocationTypeDescription -> activity_location_type_description\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_ActivityLocationTypeID" as sk_activity_location_type_id,
    "BK_ActivityLocationTypeCode" as bk_activity_location_type_code,
    "ActivityLocationTypeCategory" as activity_location_type_category,
    "ActivityLocationTypeDescription" as activity_location_type_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ActivityLocationType') }}
