{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.EncounterBilling \ndbt: source(''sus_apc_monthly'', ''EncounterBilling'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_CommissionerID -> sk_commissioner_id\n  ScheduleCode -> schedule_code\n  ContractSuffix -> contract_suffix\n  EncounterRowID -> encounter_row_id\n  TotalCost -> total_cost\n  MFFApplied -> mff_applied\n  IsShortStay -> is_short_stay\n  LongStayPayment -> long_stay_payment\n  ServiceAdjustmentApplied -> service_adjustment_applied\n  CriticalCareDayCount -> critical_care_day_count\n  ApplicableTariff -> applicable_tariff\n  SK_Date -> sk_date\n  BaseCost -> base_cost\n  Outlier_Days -> outlier_days\n  BPT_Code -> bpt_code\n  HRG_CODE -> hrg_code\n  Schedule_Description -> schedule_description\n  POD_Description -> pod_description\n  LocalCostCode -> local_cost_code\n  PBR_FINAL_TARIFF -> pbr_final_tariff\n  SPC_Days -> spc_days\n  Special_Service_Id -> special_service_id\n  RehabDays -> rehab_days\n  DelayedDischargeDays -> delayed_discharge_days\n  Adj_Final_length_Of_Stay -> adj_final_length_of_stay\n  SK_TariffTypeID -> sk_tariff_type_id\n  WardCode_At_Admission -> ward_code_at_admission\n  WardCode_At_Discharge -> ward_code_at_discharge\n  Is_Pbr -> is_pbr\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_CommissionerID" as sk_commissioner_id,
    "ScheduleCode" as schedule_code,
    "ContractSuffix" as contract_suffix,
    "EncounterRowID" as encounter_row_id,
    "TotalCost" as total_cost,
    "MFFApplied" as mff_applied,
    "IsShortStay" as is_short_stay,
    "LongStayPayment" as long_stay_payment,
    "ServiceAdjustmentApplied" as service_adjustment_applied,
    "CriticalCareDayCount" as critical_care_day_count,
    "ApplicableTariff" as applicable_tariff,
    "SK_Date" as sk_date,
    "BaseCost" as base_cost,
    "Outlier_Days" as outlier_days,
    "BPT_Code" as bpt_code,
    "HRG_CODE" as hrg_code,
    "Schedule_Description" as schedule_description,
    "POD_Description" as pod_description,
    "LocalCostCode" as local_cost_code,
    "PBR_FINAL_TARIFF" as pbr_final_tariff,
    "SPC_Days" as spc_days,
    "Special_Service_Id" as special_service_id,
    "RehabDays" as rehab_days,
    "DelayedDischargeDays" as delayed_discharge_days,
    "Adj_Final_length_Of_Stay" as adj_final_length_of_stay,
    "SK_TariffTypeID" as sk_tariff_type_id,
    "WardCode_At_Admission" as ward_code_at_admission,
    "WardCode_At_Discharge" as ward_code_at_discharge,
    "Is_Pbr" as is_pbr,
    "ActivityPeriod" as activity_period
from {{ source('sus_apc_monthly', 'EncounterBilling') }}
