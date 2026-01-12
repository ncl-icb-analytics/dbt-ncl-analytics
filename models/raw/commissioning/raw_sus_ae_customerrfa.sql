{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.CustomerRFA \ndbt: source(''sus_ae'', ''CustomerRFA'') \nColumns:\n  PRIMARYKEY_ID -> primarykey_id\n  dmicCustomerCode -> dmic_customer_code\n  dmicDerivedCCGofResidence -> dmic_derived_ccg_of_residence\n  dmicImportLogId -> dmic_import_log_id\n  dmicPrimeRecipient -> dmic_prime_recipient\n  dmicDerivedCCGofGPPrac -> dmic_derived_ccg_of_gp_prac\n  dmicShortRecipient -> dmic_short_recipient\n  dmicSubmittedCCG -> dmic_submitted_ccg\n  dmicSubmittedCCGOfResidence -> dmic_submitted_ccg_of_residence\n  dmicSubmittedProvider -> dmic_submitted_provider\n  dmicSubmittedGPPractice -> dmic_submitted_gp_practice\n  dmicDerivedGPPractice -> dmic_derived_gp_practice"
    )
}}
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmic_customer_code,
    "dmicDerivedCCGofResidence" as dmic_derived_ccg_of_residence,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicPrimeRecipient" as dmic_prime_recipient,
    "dmicDerivedCCGofGPPrac" as dmic_derived_ccg_of_gp_prac,
    "dmicShortRecipient" as dmic_short_recipient,
    "dmicSubmittedCCG" as dmic_submitted_ccg,
    "dmicSubmittedCCGOfResidence" as dmic_submitted_ccg_of_residence,
    "dmicSubmittedProvider" as dmic_submitted_provider,
    "dmicSubmittedGPPractice" as dmic_submitted_gp_practice,
    "dmicDerivedGPPractice" as dmic_derived_gp_practice
from {{ source('sus_ae', 'CustomerRFA') }}
