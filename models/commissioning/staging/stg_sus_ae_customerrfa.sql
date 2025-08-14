-- Staging model for sus_ae.CustomerRFA
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicCustomerCode" as dmiccustomercode,
    "dmicDerivedCCGofResidence" as dmicderivedccgofresidence,
    "dmicImportLogId" as dmicimportlogid,
    "dmicPrimeRecipient" as dmicprimerecipient,
    "dmicDerivedCCGofGPPrac" as dmicderivedccgofgpprac,
    "dmicShortRecipient" as dmicshortrecipient,
    "dmicSubmittedCCG" as dmicsubmittedccg,
    "dmicSubmittedCCGOfResidence" as dmicsubmittedccgofresidence,
    "dmicSubmittedProvider" as dmicsubmittedprovider,
    "dmicSubmittedGPPractice" as dmicsubmittedgppractice,
    "dmicDerivedGPPractice" as dmicderivedgppractice
from {{ source('sus_ae', 'CustomerRFA') }}
