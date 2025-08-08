-- Staging model for sus_ae.EncounterBillingRepriced
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "SK_CostingAlgorithmID" as sk_costingalgorithmid,
    "SK_SUSDataMartID" as sk_susdatamartid,
    "Month_Of_Attendance" as month_of_attendance,
    "Provider_Code" as provider_code,
    "Purchaser_Code" as purchaser_code,
    "Contract_Suffix" as contract_suffix,
    "Base_Cost" as base_cost,
    "MFF_Applied" as mff_applied,
    "Total_Cost" as total_cost,
    "POD_Description" as pod_description,
    "Schedule_Description" as schedule_description,
    "LocalCostCode" as localcostcode,
    "HRG_Code" as hrg_code,
    "AE_Department_Type" as ae_department_type
from {{ source('sus_ae', 'EncounterBillingRepriced') }}
