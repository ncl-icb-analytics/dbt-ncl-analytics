-- Staging model for dictionary.HRGTrimPoint
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_HRGID" as sk_hrgid,
    "SK_TariffTypeID" as sk_tarifftypeid,
    "FiscalYear" as fiscalyear,
    "IsElectiveStay" as iselectivestay,
    "TrimPointDays" as trimpointdays
from {{ source('dictionary', 'HRGTrimPoint') }}
