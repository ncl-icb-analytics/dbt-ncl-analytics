-- Staging model for dictionary.SUSTariffTypes
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_TariffTypeID" as sk_tarifftypeid,
    "BK_TariffType" as bk_tarifftype,
    "TariffDescription" as tariffdescription,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'SUSTariffTypes') }}
