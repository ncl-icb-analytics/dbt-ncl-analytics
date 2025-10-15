-- Raw layer model for mhsds.MHS512HospSpellComm
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "HospProvSpellId" as hosp_prov_spell_id,
    "HospProvSpellNum" as hosp_prov_spell_num,
    "OrgIDComm" as org_id_comm,
    "StartDateOrgCodeComm" as start_date_org_code_comm,
    "EndDateOrgCodeComm" as end_date_org_code_comm,
    "RecordNumber" as record_number,
    "MHS512UniqID" as mhs512_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellId" as uniq_hosp_prov_spell_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "dmIcbCommissioner" as dm_icb_commissioner,
    "dmSubIcbCommissioner" as dm_sub_icb_commissioner,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS512HospSpellComm') }}
