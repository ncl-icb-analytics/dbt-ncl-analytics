{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.SUSTariffTypes \ndbt: source(''dictionary_dbo'', ''SUSTariffTypes'') \nColumns:\n  SK_TariffTypeID -> sk_tariff_type_id\n  BK_TariffType -> bk_tariff_type\n  TariffDescription -> tariff_description\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_TariffTypeID" as sk_tariff_type_id,
    "BK_TariffType" as bk_tariff_type,
    "TariffDescription" as tariff_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'SUSTariffTypes') }}
