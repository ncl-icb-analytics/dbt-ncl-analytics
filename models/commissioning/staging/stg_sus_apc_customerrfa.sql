-- Staging model for sus_apc.CustomerRfa
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmiccustomercode,
    "dmicSubmittedCCG" as dmicsubmittedccg,
    "dmicSubmittedCCGOfResidence" as dmicsubmittedccgofresidence,
    "dmicDerivedCCG" as dmicderivedccg,
    "dmicDerivedCCGofResidence" as dmicderivedccgofresidence,
    "dmicDerivedCCGofGPPrac" as dmicderivedccgofgpprac,
    "dmicDerivedCCGofDerGPPrac" as dmicderivedccgofdergpprac,
    "dmicPDSRecipient" as dmicpdsrecipient,
    "dmicSubmittedProvider" as dmicsubmittedprovider,
    "dmicImportLogId" as dmicimportlogid,
    "dmicSubmittedGPPractice" as dmicsubmittedgppractice,
    "dmicDerivedGPPractice" as dmicderivedgppractice
from {{ source('sus_apc', 'CustomerRfa') }}
