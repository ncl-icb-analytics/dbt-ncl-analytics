-- Staging model for sus_ae.EncounterAttribution
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "ProviderCode_SollisDerived" as providercode_sollisderived,
    "CommissionerCode_SollisDerived" as commissionercode_sollisderived,
    "ProviderCode_ProviderDerived" as providercode_providerderived,
    "CommissionerCode_ProviderDerived" as commissionercode_providerderived,
    "CommissionerCode_DerivedFromGPCode" as commissionercode_derivedfromgpcode,
    "CommissionerCode_DerivedFromPracticeCode" as commissionercode_derivedfrompracticecode,
    "SUSCommissionerCode_DerivedFromResidence" as suscommissionercode_derivedfromresidence,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterAttribution') }}
