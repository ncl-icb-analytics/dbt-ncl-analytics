-- Staging model for dictionary.UnitMapping
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_UnitID" as sk_unitid,
    "UnitLabel" as unitlabel
from {{ source('dictionary', 'UnitMapping') }}
