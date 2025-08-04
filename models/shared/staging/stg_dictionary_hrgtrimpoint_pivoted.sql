-- Staging model for dictionary.HRGTrimPoint_Pivoted
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_HRGID" as sk_hrgid,
    "TariffType" as tarifftype,
    "TariffTypeDesc" as tarifftypedesc,
    "FiscalYear" as fiscalyear,
    "Elective_TrimPointDays" as elective_trimpointdays,
    "Non-Elective_TrimPointDays" as non_elective_trimpointdays
from {{ source('dictionary', 'HRGTrimPoint_Pivoted') }}
