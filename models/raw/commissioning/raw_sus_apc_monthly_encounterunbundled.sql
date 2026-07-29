{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.EncounterUnbundled \ndbt: source(''sus_apc_monthly'', ''EncounterUnbundled'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  HRG -> hrg\n  UnbundledNo -> unbundled_no\n  POD_Description -> pod_description\n  Schedule_Description -> schedule_description\n  Provider -> provider\n  Purchaser -> purchaser\n  Suffix -> suffix\n  Cost -> cost\n  CostCode -> cost_code\n  IsNational -> is_national\n  MonthOfActivity -> month_of_activity\n  BaseTariff -> base_tariff\n  MFF -> mff\n  ApplyMFF -> apply_mff\n  NumberOfDays -> number_of_days\n  TariffType -> tariff_type\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "HRG" as hrg,
    "UnbundledNo" as unbundled_no,
    "POD_Description" as pod_description,
    "Schedule_Description" as schedule_description,
    "Provider" as provider,
    "Purchaser" as purchaser,
    "Suffix" as suffix,
    "Cost" as cost,
    "CostCode" as cost_code,
    "IsNational" as is_national,
    "MonthOfActivity" as month_of_activity,
    "BaseTariff" as base_tariff,
    "MFF" as mff,
    "ApplyMFF" as apply_mff,
    "NumberOfDays" as number_of_days,
    "TariffType" as tariff_type,
    "ActivityPeriod" as activity_period
from {{ source('sus_apc_monthly', 'EncounterUnbundled') }}
