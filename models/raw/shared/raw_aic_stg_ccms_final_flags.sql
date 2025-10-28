-- Raw layer model for aic.STG_CCMS__FINAL_FLAGS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PERSON_ID" as person_id,
    "ALCOHOLPROBLEMS" as alcoholproblems,
    "ANXIETYORDEPRESSION" as anxietyordepression,
    "ATRIALFIBRILLATION" as atrialfibrillation,
    "CANCERINTHELAST5YEARS" as cancerinthelast5_years,
    "CHRONICKIDNEYDISEASE" as chronickidneydisease,
    "CHRONICLIVERDISEASEANDVIRALHEPATITIS" as chronicliverdiseaseandviralhepatitis,
    "CONSTIPATION" as constipation,
    "COPD" as copd,
    "DEMENTIA" as dementia,
    "DIABETES" as diabetes,
    "DISORDEROFPROSTATE" as disorderofprostate,
    "EPILEPSY" as epilepsy,
    "HEARTFAILURE" as heartfailure,
    "IRRITABLEBOWELSYNDROME" as irritablebowelsyndrome,
    "LEARNINGDISABILITY" as learningdisability,
    "MULTIPLESCLEROSIS" as multiplesclerosis,
    "PAINFULCONDITION" as painfulcondition,
    "PARKINSONISM" as parkinsonism,
    "PERIPHVASCDISEASELEG" as periphvascdiseaseleg,
    "PSYCHOACTIVESUBSTANCEMISUSE" as psychoactivesubstancemisuse,
    "SCHIZOPHRENIAORBIPOLARDISORDER" as schizophreniaorbipolardisorder
from {{ source('aic', 'STG_CCMS__FINAL_FLAGS') }}
