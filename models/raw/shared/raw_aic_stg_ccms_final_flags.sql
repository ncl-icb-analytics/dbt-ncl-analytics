{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.STG_CCMS__FINAL_FLAG \ndbt: source(''aic'', ''STG_CCMS__FINAL_FLAGS'') \nColumns:\n  PERSON_ID -> person_id\n  ALCOHOLPROBLEMS -> alcoholproblems\n  ANXIETYORDEPRESSION -> anxietyordepression\n  ATRIALFIBRILLATION -> atrialfibrillation\n  CANCERINTHELAST5YEARS -> cancerinthelast5_years\n  CHRONICKIDNEYDISEASE -> chronickidneydisease\n  CHRONICLIVERDISEASEANDVIRALHEPATITIS -> chronicliverdiseaseandviralhepatitis\n  CONSTIPATION -> constipation\n  COPD -> copd\n  DEMENTIA -> dementia\n  DIABETES -> diabetes\n  DISORDEROFPROSTATE -> disorderofprostate\n  EPILEPSY -> epilepsy\n  HEARTFAILURE -> heartfailure\n  IRRITABLEBOWELSYNDROME -> irritablebowelsyndrome\n  LEARNINGDISABILITY -> learningdisability\n  MULTIPLESCLEROSIS -> multiplesclerosis\n  PAINFULCONDITION -> painfulcondition\n  PARKINSONISM -> parkinsonism\n  PERIPHVASCDISEASELEG -> periphvascdiseaseleg\n  PSYCHOACTIVESUBSTANCEMISUSE -> psychoactivesubstancemisuse\n  SCHIZOPHRENIAORBIPOLARDISORDER -> schizophreniaorbipolardisorder"
    )
}}
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
