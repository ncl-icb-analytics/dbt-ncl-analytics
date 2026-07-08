{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterAttribution \ndbt: source(''sus_apc_monthly'', ''EncounterAttribution'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  ProviderCode_SollisDerived -> provider_code_sollis_derived\n  ProviderCode_SollisDerived -> provider_code_sollis_derived_1\n  CommissionerCode_SollisDerived -> commissioner_code_sollis_derived\n  CommissionerCode_SollisDerived -> commissioner_code_sollis_derived_1\n  ProviderCode_ProviderDerived -> provider_code_provider_derived\n  ProviderCode_ProviderDerived -> provider_code_provider_derived_1\n  CommissionerCode_ProviderDerived -> commissioner_code_provider_derived\n  CommissionerCode_ProviderDerived -> commissioner_code_provider_derived_1\n  CommissionerCode_DerivedFromGPCode -> commissioner_code_derived_from_gp_code\n  CommissionerCode_DerivedFromGPCode -> commissioner_code_derived_from_gp_code_1\n  CommissionerCode_DerivedFromPracticeCode -> commissioner_code_derived_from_practice_code\n  CommissionerCode_DerivedFromPracticeCode -> commissioner_code_derived_from_practice_code_1\n  SUSCommissionerCode_DerivedFromResidence -> sus_commissioner_code_derived_from_residence\n  SUSCommissionerCode_DerivedFromResidence -> sus_commissioner_code_derived_from_residence_1\n  ActivityPeriod -> activity_period\n  ActivityPeriod -> activity_period_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "ProviderCode_SollisDerived" as provider_code_sollis_derived,
    "ProviderCode_SollisDerived" as provider_code_sollis_derived_1,
    "CommissionerCode_SollisDerived" as commissioner_code_sollis_derived,
    "CommissionerCode_SollisDerived" as commissioner_code_sollis_derived_1,
    "ProviderCode_ProviderDerived" as provider_code_provider_derived,
    "ProviderCode_ProviderDerived" as provider_code_provider_derived_1,
    "CommissionerCode_ProviderDerived" as commissioner_code_provider_derived,
    "CommissionerCode_ProviderDerived" as commissioner_code_provider_derived_1,
    "CommissionerCode_DerivedFromGPCode" as commissioner_code_derived_from_gp_code,
    "CommissionerCode_DerivedFromGPCode" as commissioner_code_derived_from_gp_code_1,
    "CommissionerCode_DerivedFromPracticeCode" as commissioner_code_derived_from_practice_code,
    "CommissionerCode_DerivedFromPracticeCode" as commissioner_code_derived_from_practice_code_1,
    "SUSCommissionerCode_DerivedFromResidence" as sus_commissioner_code_derived_from_residence,
    "SUSCommissionerCode_DerivedFromResidence" as sus_commissioner_code_derived_from_residence_1,
    "ActivityPeriod" as activity_period,
    "ActivityPeriod" as activity_period_1
from {{ source('sus_apc_monthly', 'EncounterAttribution') }}
