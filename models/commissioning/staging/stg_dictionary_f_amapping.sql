-- Staging model for dictionary.F&AMapping
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PODID" as sk_podid,
    "SK_LevelID" as sk_levelid,
    "Finance/Activity" as finance_activity,
    "Description" as description
from {{ source('dictionary', 'F&AMapping') }}
