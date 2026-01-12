{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS515RestrictiveInterventType \ndbt: source(''mhsds'', ''MHS515RestrictiveInterventType'') \nColumns:\n  SK -> sk\n  RestrictiveIntIncID -> restrictive_int_inc_id\n  RestrictiveIntTypeID -> restrictive_int_type_id\n  RestrictiveIntType -> restrictive_int_type\n  StartDateRestrictiveIntType -> start_date_restrictive_int_type\n  StartTimeRestrictiveIntType -> start_time_restrictive_int_type\n  EndDateRestrictiveIntType -> end_date_restrictive_int_type\n  EndTimeRestrictiveIntType -> end_time_restrictive_int_type\n  RestraintInjuryPatient -> restraint_injury_patient\n  RestraintInjuryCarePers -> restraint_injury_care_pers\n  RestraintInjuryOtherPers -> restraint_injury_other_pers\n  RecordNumber -> record_number\n  MHS515UniqID -> mhs515_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqHospProvSpellNum -> uniq_hosp_prov_spell_num\n  UniqWardStayID -> uniq_ward_stay_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  UniqRestrictiveIntIncID -> uniq_restrictive_int_inc_id\n  UniqRestrictiveIntTypeID -> uniq_restrictive_int_type_id\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "RestrictiveIntIncID" as restrictive_int_inc_id,
    "RestrictiveIntTypeID" as restrictive_int_type_id,
    "RestrictiveIntType" as restrictive_int_type,
    "StartDateRestrictiveIntType" as start_date_restrictive_int_type,
    "StartTimeRestrictiveIntType" as start_time_restrictive_int_type,
    "EndDateRestrictiveIntType" as end_date_restrictive_int_type,
    "EndTimeRestrictiveIntType" as end_time_restrictive_int_type,
    "RestraintInjuryPatient" as restraint_injury_patient,
    "RestraintInjuryCarePers" as restraint_injury_care_pers,
    "RestraintInjuryOtherPers" as restraint_injury_other_pers,
    "RecordNumber" as record_number,
    "MHS515UniqID" as mhs515_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqWardStayID" as uniq_ward_stay_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "UniqRestrictiveIntIncID" as uniq_restrictive_int_inc_id,
    "UniqRestrictiveIntTypeID" as uniq_restrictive_int_type_id,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS515RestrictiveInterventType') }}
