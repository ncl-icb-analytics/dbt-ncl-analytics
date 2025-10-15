-- Raw layer model for mhsds.MHS504DelayedDischarge
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "HospProvSpellID" as hosp_prov_spell_id,
    "HospProvSpellNum" as hosp_prov_spell_num,
    "StartDateDelayDisch" as start_date_delay_disch,
    "EndDateDelayDisch" as end_date_delay_disch,
    "DelayDischReason" as delay_disch_reason,
    "AttribToIndic" as attrib_to_indic,
    "OrgIDRespLADelayDisch" as org_id_resp_la_delay_disch,
    "RecordNumber" as record_number,
    "MHS504UniqID" as mhs504_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "BedDaysDDRP" as bed_days_ddrp,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS504DelayedDischarge') }}
