-- Staging model for dictionary.Wards
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_WardID" as sk_wardid,
    "WardCode" as wardcode,
    "WardName" as wardname,
    "GeoCentroid_text" as geocentroid_text,
    "GeoCentroid" as geocentroid,
    "WardShape_text" as wardshape_text,
    "WardShape" as wardshape,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Wards') }}
