-- Staging model for dictionary.CarerSupportIndicator
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_CarerSupportIndicatorID" as sk_carersupportindicatorid,
    "BK_CarerSupportIndicator" as bk_carersupportindicator,
    "CarerSupportIndicator" as carersupportindicator
from {{ source('dictionary', 'CarerSupportIndicator') }}
