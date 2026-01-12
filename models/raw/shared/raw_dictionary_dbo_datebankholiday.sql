{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.DateBankHoliday \ndbt: source(''dictionary_dbo'', ''DateBankHoliday'') \nColumns:\n  SK_Date -> sk_date\n  FullDate -> full_date\n  Holiday -> holiday\n  InEnglandAndWales -> in_england_and_wales\n  InNorthernIreland -> in_northern_ireland\n  InScotland -> in_scotland"
    )
}}
select
    "SK_Date" as sk_date,
    "FullDate" as full_date,
    "Holiday" as holiday,
    "InEnglandAndWales" as in_england_and_wales,
    "InNorthernIreland" as in_northern_ireland,
    "InScotland" as in_scotland
from {{ source('dictionary_dbo', 'DateBankHoliday') }}
