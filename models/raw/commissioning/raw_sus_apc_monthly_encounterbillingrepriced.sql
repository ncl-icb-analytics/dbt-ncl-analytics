{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterBillingRepriced \ndbt: source(''sus_apc_monthly'', ''EncounterBillingRepriced'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id_1\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id_1\n  Month_Of_Attendance -> month_of_attendance\n  Month_Of_Attendance -> month_of_attendance_1\n  Provider_Code -> provider_code\n  Provider_Code -> provider_code_1\n  Purchaser_Code -> purchaser_code\n  Purchaser_Code -> purchaser_code_1\n  Contract_Suffix -> contract_suffix\n  Contract_Suffix -> contract_suffix_1\n  Base_Cost -> base_cost\n  Base_Cost -> base_cost_1\n  MFF_Applied -> mff_applied\n  MFF_Applied -> mff_applied_1\n  Total_Cost -> total_cost\n  Total_Cost -> total_cost_1\n  POD_Description -> pod_description\n  POD_Description -> pod_description_1\n  Schedule_Description -> schedule_description\n  Schedule_Description -> schedule_description_1\n  LocalCostCode -> local_cost_code\n  LocalCostCode -> local_cost_code_1\n  HRG_Code -> hrg_code\n  HRG_Code -> hrg_code_1\n  AE_Department_Type -> ae_department_type\n  AE_Department_Type -> ae_department_type_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "SK_CostingAlgorithmID" as sk_costing_algorithm_id,
    "SK_CostingAlgorithmID" as sk_costing_algorithm_id_1,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id_1,
    "Month_Of_Attendance" as month_of_attendance,
    "Month_Of_Attendance" as month_of_attendance_1,
    "Provider_Code" as provider_code,
    "Provider_Code" as provider_code_1,
    "Purchaser_Code" as purchaser_code,
    "Purchaser_Code" as purchaser_code_1,
    "Contract_Suffix" as contract_suffix,
    "Contract_Suffix" as contract_suffix_1,
    "Base_Cost" as base_cost,
    "Base_Cost" as base_cost_1,
    "MFF_Applied" as mff_applied,
    "MFF_Applied" as mff_applied_1,
    "Total_Cost" as total_cost,
    "Total_Cost" as total_cost_1,
    "POD_Description" as pod_description,
    "POD_Description" as pod_description_1,
    "Schedule_Description" as schedule_description,
    "Schedule_Description" as schedule_description_1,
    "LocalCostCode" as local_cost_code,
    "LocalCostCode" as local_cost_code_1,
    "HRG_Code" as hrg_code,
    "HRG_Code" as hrg_code_1,
    "AE_Department_Type" as ae_department_type,
    "AE_Department_Type" as ae_department_type_1
from {{ source('sus_apc_monthly', 'EncounterBillingRepriced') }}
