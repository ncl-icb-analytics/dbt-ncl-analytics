-- Staging model for dictionary_dbo.DateBankHoliday
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_Date" as sk_date,
    "FullDate" as full_date,
    "Holiday" as holiday,
    "InEnglandAndWales" as in_england_and_wales,
    "InNorthernIreland" as in_northern_ireland,
    "InScotland" as in_scotland
from {{ source('dictionary_dbo', 'DateBankHoliday') }}
