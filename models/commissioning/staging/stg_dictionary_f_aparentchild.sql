-- Staging model for dictionary.F&AParentChild
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PARENT_PODID" as sk_parent_podid,
    "SK_CHILD_PODID" as sk_child_podid
from {{ source('dictionary', 'F&AParentChild') }}
