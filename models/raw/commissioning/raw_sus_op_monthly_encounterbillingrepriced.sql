{{
    config(
        description="Raw layer (SUS Monthly outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_OP.EncounterBillingRepriced \ndbt: source(''sus_op_monthly'', ''EncounterBillingRepriced'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  Month_Of_Attendance -> month_of_attendance\n  Provider_Code -> provider_code\n  Purchaser_Code -> purchaser_code\n  Contract_Suffix -> contract_suffix\n  Base_Cost -> base_cost\n  Total_Cost -> total_cost\n  MFF_Applied -> mff_applied\n  POD_Description -> pod_description\n  Schedule_Description -> schedule_description\n  LocalCostCode -> local_cost_code\n  HRG_Code -> hrg_code\n  ServiceLine -> service_line"
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
    "Total_Cost" as total_cost,
    "MFF_Applied" as mff_applied,
    "POD_Description" as pod_description,
    "Schedule_Description" as schedule_description,
    "LocalCostCode" as local_cost_code,
    "HRG_Code" as hrg_code,
    "ServiceLine" as service_line
from {{ source('sus_op_monthly', 'EncounterBillingRepriced') }}
