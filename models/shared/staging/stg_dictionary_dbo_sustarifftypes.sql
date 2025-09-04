-- Staging model for dictionary_dbo.SUSTariffTypes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_TariffTypeID" as sk_tariff_type_id,
    "BK_TariffType" as bk_tariff_type,
    "TariffDescription" as tariff_description,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'SUSTariffTypes') }}
