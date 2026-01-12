{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS002GP \ndbt: source(''mhsds'', ''MHS002GP'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  GMPReg -> gmp_reg\n  GMPCodeReg -> gmp_code_reg\n  StartDateGMPRegistration -> start_date_gmp_registration\n  EndDateGMPRegistration -> end_date_gmp_registration\n  OrgIDGPPrac -> org_idgp_prac\n  RecordNumber -> record_number\n  MHS002UniqID -> mhs002_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  GPDistanceHome -> gp_distance_home\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  LADistrictAuthGPPractice -> la_district_auth_gp_practice\n  OrgIDICBGPPractice -> org_idicbgp_practice\n  OrgIDICSGPPractice -> org_idicsgp_practice\n  OrgIDSubICBLocGP -> org_id_sub_icb_loc_gp\n  OrgIDCCGGPPractice -> org_idccggp_practice\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  dmIcbRegistrationSubmitted -> dm_icb_registration_submitted\n  dmSubIcbRegistrationSubmitted -> dm_sub_icb_registration_submitted\n  dmCommissionerDerivationReason -> dm_commissioner_derivation_reason\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "GMPReg" as gmp_reg,
    "GMPCodeReg" as gmp_code_reg,
    "StartDateGMPRegistration" as start_date_gmp_registration,
    "EndDateGMPRegistration" as end_date_gmp_registration,
    "OrgIDGPPrac" as org_idgp_prac,
    "RecordNumber" as record_number,
    "MHS002UniqID" as mhs002_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "GPDistanceHome" as gp_distance_home,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "LADistrictAuthGPPractice" as la_district_auth_gp_practice,
    "OrgIDICBGPPractice" as org_idicbgp_practice,
    "OrgIDICSGPPractice" as org_idicsgp_practice,
    "OrgIDSubICBLocGP" as org_id_sub_icb_loc_gp,
    "OrgIDCCGGPPractice" as org_idccggp_practice,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "dmIcbRegistrationSubmitted" as dm_icb_registration_submitted,
    "dmSubIcbRegistrationSubmitted" as dm_sub_icb_registration_submitted,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS002GP') }}
