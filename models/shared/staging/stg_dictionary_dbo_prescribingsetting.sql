-- Staging model for dictionary_dbo.PrescribingSetting
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PrescribingSettingID" as sk_prescribing_setting_id,
    "BK_PrescribingSetting" as bk_prescribing_setting,
    "PrescribingSetting" as prescribing_setting
from {{ source('dictionary_dbo', 'PrescribingSetting') }}
