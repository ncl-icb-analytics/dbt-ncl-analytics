{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PrescribingSetting \ndbt: source(''dictionary_dbo'', ''PrescribingSetting'') \nColumns:\n  SK_PrescribingSettingID -> sk_prescribing_setting_id\n  BK_PrescribingSetting -> bk_prescribing_setting\n  PrescribingSetting -> prescribing_setting"
    )
}}
select
    "SK_PrescribingSettingID" as sk_prescribing_setting_id,
    "BK_PrescribingSetting" as bk_prescribing_setting,
    "PrescribingSetting" as prescribing_setting
from {{ source('dictionary_dbo', 'PrescribingSetting') }}
