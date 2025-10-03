-- Raw layer model for sus_apc.CustomerRfa
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmic_customer_code,
    "dmicSubmittedCCG" as dmic_submitted_ccg,
    "dmicSubmittedCCGOfResidence" as dmic_submitted_ccg_of_residence,
    "dmicDerivedCCG" as dmic_derived_ccg,
    "dmicDerivedCCGofResidence" as dmic_derived_ccg_of_residence,
    "dmicDerivedCCGofGPPrac" as dmic_derived_ccg_of_gp_prac,
    "dmicDerivedCCGofDerGPPrac" as dmic_derived_ccg_of_der_gp_prac,
    "dmicPDSRecipient" as dmic_pds_recipient,
    "dmicSubmittedProvider" as dmic_submitted_provider,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSubmittedGPPractice" as dmic_submitted_gp_practice,
    "dmicDerivedGPPractice" as dmic_derived_gp_practice
from {{ source('sus_apc', 'CustomerRfa') }}
