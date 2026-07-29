{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.EncounterBillingRepriced \ndbt: source(''sus_apc_monthly'', ''EncounterBillingRepriced'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  Month_Of_Attendance -> month_of_attendance\n  Provider_Code -> provider_code\n  Purchaser_Code -> purchaser_code\n  Contract_Suffix -> contract_suffix\n  Base_Cost -> base_cost\n  MFF_Applied -> mff_applied\n  Total_Cost -> total_cost\n  Short_Stay -> short_stay\n  LongStayPayment -> long_stay_payment\n  Service_Adjustment_if_Applied -> service_adjustment_if_applied\n  Critical_Care_Days -> critical_care_days\n  Outlier_Days -> outlier_days\n  BPT_Code -> bpt_code\n  HRG_Code -> hrg_code\n  Schedule_Description -> schedule_description\n  POD_Description -> pod_description\n  LocalCostCode -> local_cost_code\n  SPC_Days -> spc_days\n  Special_Service_Id -> special_service_id\n  RehabDays -> rehab_days\n  DelayedDischargeDays -> delayed_discharge_days\n  Adj_Final_Length_Of_Stay -> adj_final_length_of_stay\n  FCEServiceLine -> fce_service_line\n  SpellServiceLine -> spell_service_line"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_CostingAlgorithmID" as sk_costing_algorithm_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "Month_Of_Attendance" as month_of_attendance,
    "Provider_Code" as provider_code,
    "Purchaser_Code" as purchaser_code,
    "Contract_Suffix" as contract_suffix,
    "Base_Cost" as base_cost,
    "MFF_Applied" as mff_applied,
    "Total_Cost" as total_cost,
    "Short_Stay" as short_stay,
    "LongStayPayment" as long_stay_payment,
    "Service_Adjustment_if_Applied" as service_adjustment_if_applied,
    "Critical_Care_Days" as critical_care_days,
    "Outlier_Days" as outlier_days,
    "BPT_Code" as bpt_code,
    "HRG_Code" as hrg_code,
    "Schedule_Description" as schedule_description,
    "POD_Description" as pod_description,
    "LocalCostCode" as local_cost_code,
    "SPC_Days" as spc_days,
    "Special_Service_Id" as special_service_id,
    "RehabDays" as rehab_days,
    "DelayedDischargeDays" as delayed_discharge_days,
    "Adj_Final_Length_Of_Stay" as adj_final_length_of_stay,
    "FCEServiceLine" as fce_service_line,
    "SpellServiceLine" as spell_service_line
from {{ source('sus_apc_monthly', 'EncounterBillingRepriced') }}
