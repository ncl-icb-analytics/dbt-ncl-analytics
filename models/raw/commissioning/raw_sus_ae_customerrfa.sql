-- Raw layer model for sus_ae.CustomerRFA
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
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
