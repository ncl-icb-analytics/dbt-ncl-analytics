{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.CustomerRfa \ndbt: source(''sus_apc'', ''CustomerRfa'') \nColumns:\n  PRIMARYKEY_ID -> primarykey_id\n  dmicCustomerCode -> dmic_customer_code\n  dmicSubmittedCCG -> dmic_submitted_ccg\n  dmicSubmittedCCGOfResidence -> dmic_submitted_ccg_of_residence\n  dmicDerivedCCG -> dmic_derived_ccg\n  dmicDerivedCCGofResidence -> dmic_derived_ccg_of_residence\n  dmicDerivedCCGofGPPrac -> dmic_derived_ccg_of_gp_prac\n  dmicDerivedCCGofDerGPPrac -> dmic_derived_ccg_of_der_gp_prac\n  dmicPDSRecipient -> dmic_pds_recipient\n  dmicSubmittedProvider -> dmic_submitted_provider\n  dmicImportLogId -> dmic_import_log_id\n  dmicSubmittedGPPractice -> dmic_submitted_gp_practice\n  dmicDerivedGPPractice -> dmic_derived_gp_practice"
    )
}}
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
