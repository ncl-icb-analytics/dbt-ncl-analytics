-- Staging model for dictionary.DateBankHoliday
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_Date" as sk_date,
    "FullDate" as fulldate,
    "Holiday" as holiday,
    "InEnglandAndWales" as inenglandandwales,
    "InNorthernIreland" as innorthernireland,
    "InScotland" as inscotland
from {{ source('dictionary', 'DateBankHoliday') }}
