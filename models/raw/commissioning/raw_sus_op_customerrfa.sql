-- Raw layer model for sus_op.CustomerRFA
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity
-- This is a 1:1 passthrough from source with standardized column names
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
