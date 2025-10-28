-- Raw layer model for mhsds.MHS006MHCareCoord
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "StartDateAssCareCoord" as start_date_ass_care_coord,
    "CareProfLocalId" as care_prof_local_id,
    "EndDateAssCareCoord" as end_date_ass_care_coord,
    "CareProfServOrTeamTypeAssoc" as care_prof_serv_or_team_type_assoc,
    "RecordNumber" as record_number,
    "MHS006UniqID" as mhs006_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "InactTimeCC" as inact_time_cc,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS006MHCareCoord') }}
