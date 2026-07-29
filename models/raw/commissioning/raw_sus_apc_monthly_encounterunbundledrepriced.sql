{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.EncounterUnbundledRepriced \ndbt: source(''sus_apc_monthly'', ''EncounterUnbundledRepriced'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_CostingAlgorithmID -> sk_costing_algorithm_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  UnbundledNo -> unbundled_no\n  Month_Of_Attendance -> month_of_attendance\n  Provider_Code -> provider_code\n  Purchaser_Code -> purchaser_code\n  Contract_Suffix -> contract_suffix\n  IsNational -> is_national\n  Base_Cost -> base_cost\n  MFF -> mff\n  ApplyMFF -> apply_mff\n  Total_Cost -> total_cost\n  HRG_Code -> hrg_code\n  POD_Description -> pod_description\n  Schedule_Description -> schedule_description\n  LocalCostCode -> local_cost_code\n  Number_Of_Days -> number_of_days"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_CostingAlgorithmID" as sk_costing_algorithm_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "UnbundledNo" as unbundled_no,
    "Month_Of_Attendance" as month_of_attendance,
    "Provider_Code" as provider_code,
    "Purchaser_Code" as purchaser_code,
    "Contract_Suffix" as contract_suffix,
    "IsNational" as is_national,
    "Base_Cost" as base_cost,
    "MFF" as mff,
    "ApplyMFF" as apply_mff,
    "Total_Cost" as total_cost,
    "HRG_Code" as hrg_code,
    "POD_Description" as pod_description,
    "Schedule_Description" as schedule_description,
    "LocalCostCode" as local_cost_code,
    "Number_Of_Days" as number_of_days
from {{ source('sus_apc_monthly', 'EncounterUnbundledRepriced') }}
