-- Staging model for sus_op.CustomerRFA
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "dmicDerivedCCGofResidence" as dmicderivedccgofresidence,
    "dmicPrimeRecipient" as dmicprimerecipient,
    "dmicSubmittedCCG" as dmicsubmittedccg,
    "dmicSubmittedCCGOfResidence" as dmicsubmittedccgofresidence,
    "dmicDerivedCCG" as dmicderivedccg,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmiccustomercode,
    "dmicSubmittedProvider" as dmicsubmittedprovider,
    "dmicSubmittedGPPractice" as dmicsubmittedgppractice,
    "dmicDerivedCCGofGPPrac" as dmicderivedccgofgpprac,
    "dmicDerivedCCGofDerGPPrac" as dmicderivedccgofdergpprac,
    "dmicShortRecipient" as dmicshortrecipient,
    "dmicDerivedGPPractice" as dmicderivedgppractice,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_op', 'CustomerRFA') }}
