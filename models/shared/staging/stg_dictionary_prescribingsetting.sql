-- Staging model for dictionary.PrescribingSetting
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PrescribingSettingID" as sk_prescribingsettingid,
    "BK_PrescribingSetting" as bk_prescribingsetting,
    "PrescribingSetting" as prescribingsetting
from {{ source('dictionary', 'PrescribingSetting') }}
