{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS503AssignedCareProf \ndbt: source(''mhsds'', ''MHS503AssignedCareProf'') \nColumns:\n  SK -> sk\n  HospProvSpellID -> hosp_prov_spell_id\n  HospProvSpellNum -> hosp_prov_spell_num\n  CareProfLocalId -> care_prof_local_id\n  StartDateAssCareProf -> start_date_ass_care_prof\n  EndDateAssCareProf -> end_date_ass_care_prof\n  TreatFuncCodeMH -> treat_func_code_mh\n  RecordNumber -> record_number\n  MHS503UniqID -> mhs503_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqHospProvSpellNum -> uniq_hosp_prov_spell_num\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "HospProvSpellID" as hosp_prov_spell_id,
    "HospProvSpellNum" as hosp_prov_spell_num,
    "CareProfLocalId" as care_prof_local_id,
    "StartDateAssCareProf" as start_date_ass_care_prof,
    "EndDateAssCareProf" as end_date_ass_care_prof,
    "TreatFuncCodeMH" as treat_func_code_mh,
    "RecordNumber" as record_number,
    "MHS503UniqID" as mhs503_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS503AssignedCareProf') }}
