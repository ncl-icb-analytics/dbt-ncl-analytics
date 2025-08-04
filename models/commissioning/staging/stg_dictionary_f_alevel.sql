-- Staging model for dictionary.F&ALevel
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_LevelID" as sk_levelid,
    "LevelName" as levelname
from {{ source('dictionary', 'F&ALevel') }}
