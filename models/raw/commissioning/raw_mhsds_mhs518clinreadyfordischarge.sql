{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS518ClinReadyforDischarge \ndbt: source(''mhsds'', ''MHS518ClinReadyforDischarge'') \nColumns:\n  SK -> sk\n  HospProvSpellID -> hosp_prov_spell_id\n  StartDateClinReadyforDisch -> start_date_clin_readyfor_disch\n  EndDateClinReadyforDisch -> end_date_clin_readyfor_disch\n  ClinReadyforDischDelayReason -> clin_readyfor_disch_delay_reason\n  AttribToIndic -> attrib_to_indic\n  OrgIDRespLAClinReadyforDisch -> org_id_resp_la_clin_readyfor_disch\n  RecordNumber -> record_number\n  MHS518UniqID -> mhs518_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "HospProvSpellID" as hosp_prov_spell_id,
    "StartDateClinReadyforDisch" as start_date_clin_readyfor_disch,
    "EndDateClinReadyforDisch" as end_date_clin_readyfor_disch,
    "ClinReadyforDischDelayReason" as clin_readyfor_disch_delay_reason,
    "AttribToIndic" as attrib_to_indic,
    "OrgIDRespLAClinReadyforDisch" as org_id_resp_la_clin_readyfor_disch,
    "RecordNumber" as record_number,
    "MHS518UniqID" as mhs518_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
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
from {{ source('mhsds', 'MHS518ClinReadyforDischarge') }}
