-- Staging model for sus_ae.EncounterBilling
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_CommissionerID" as sk_commissionerid,
    "ScheduleCode" as schedulecode,
    "ContractSuffix" as contractsuffix,
    "EncounterRowID" as encounterrowid,
    "TotalCost" as totalcost,
    "MFFApplied" as mffapplied,
    "IsShortStay" as isshortstay,
    "LongStayPayment" as longstaypayment,
    "ServiceAdjustmentApplied" as serviceadjustmentapplied,
    "CriticalCareDayCount" as criticalcaredaycount,
    "ApplicableTariff" as applicabletariff,
    "SK_Date" as sk_date,
    "BaseCost" as basecost,
    "Schedule_Description" as schedule_description,
    "POD_Description" as pod_description,
    "LocalCostCode" as localcostcode,
    "PBR_FINAL_TARIFF" as pbr_final_tariff,
    "SK_TariffTypeID" as sk_tarifftypeid,
    "Is_Pbr" as is_pbr,
    "HRG_Code" as hrg_code,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterBilling') }}
