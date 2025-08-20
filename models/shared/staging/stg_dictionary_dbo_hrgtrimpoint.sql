-- Staging model for dictionary_dbo.HRGTrimPoint
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_HRGID" as sk_hrgid,
    "SK_TariffTypeID" as sk_tarifftypeid,
    "FiscalYear" as fiscalyear,
    "IsElectiveStay" as iselectivestay,
    "TrimPointDays" as trimpointdays
from {{ source('dictionary_dbo', 'HRGTrimPoint') }}
