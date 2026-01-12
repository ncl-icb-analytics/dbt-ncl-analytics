{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.CustomerRFA \ndbt: source(''sus_op'', ''CustomerRFA'') \nColumns:\n  PRIMARYKEY_ID -> primarykey_id\n  dmicCustomerCode -> dmic_customer_code\n  dmicPrimeRecipient -> dmic_prime_recipient\n  dmicSubmittedCCG -> dmic_submitted_ccg\n  dmicSubmittedCCGOfResidence -> dmic_submitted_ccg_of_residence\n  dmicDerivedCCG -> dmic_derived_ccg\n  dmicDerivedCCGofResidence -> dmic_derived_ccg_of_residence\n  dmicDerivedCCGofGPPrac -> dmic_derived_ccg_of_gp_prac\n  dmicDerivedCCGofDerGPPrac -> dmic_derived_ccg_of_der_gp_prac\n  dmicShortRecipient -> dmic_short_recipient\n  dmicSubmittedProvider -> dmic_submitted_provider\n  dmicSubmittedGPPractice -> dmic_submitted_gp_practice\n  dmicDerivedGPPractice -> dmic_derived_gp_practice\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmic_customer_code,
    "dmicPrimeRecipient" as dmic_prime_recipient,
    "dmicSubmittedCCG" as dmic_submitted_ccg,
    "dmicSubmittedCCGOfResidence" as dmic_submitted_ccg_of_residence,
    "dmicDerivedCCG" as dmic_derived_ccg,
    "dmicDerivedCCGofResidence" as dmic_derived_ccg_of_residence,
    "dmicDerivedCCGofGPPrac" as dmic_derived_ccg_of_gp_prac,
    "dmicDerivedCCGofDerGPPrac" as dmic_derived_ccg_of_der_gp_prac,
    "dmicShortRecipient" as dmic_short_recipient,
    "dmicSubmittedProvider" as dmic_submitted_provider,
    "dmicSubmittedGPPractice" as dmic_submitted_gp_practice,
    "dmicDerivedGPPractice" as dmic_derived_gp_practice,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'CustomerRFA') }}
