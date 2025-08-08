-- Staging model for sus_ae.EncounterBillingAttribution
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Provider_CommissionerCode" as provider_commissionercode,
    "Provider_PracticeCode" as provider_practicecode,
    "SUS_CommissionerCode_GPPractice" as sus_commissionercode_gppractice,
    "SUS_CommissionerCode_Residence" as sus_commissionercode_residence,
    "SUS_PracticeCode" as sus_practicecode,
    "SOLLIS_CommissionerCode" as sollis_commissionercode,
    "DWH_CommissionerCode" as dwh_commissionercode,
    "DWH_PracticeCode" as dwh_practicecode,
    "Provider_GMPCode" as provider_gmpcode,
    "SK_CommissionerID_DWH_Commissioner" as sk_commissionerid_dwh_commissioner,
    "SK_ServiceProviderID_DWH_Practice" as sk_serviceproviderid_dwh_practice,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterBillingAttribution') }}
