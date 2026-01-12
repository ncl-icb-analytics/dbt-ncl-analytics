{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP601MedicalHistory \ndbt: source(''csds'', ''CYP601MedicalHistory'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  DIAGNOSIS SCHEME IN USE (COMMUNITY CARE) -> diagnosis_scheme_in_use_community_care\n  DIAGNOSIS SCHEME IN USE -> diagnosis_scheme_in_use\n  PREVIOUS DIAGNOSIS (CODED CLINICAL ENTRY) -> previous_diagnosis_coded_clinical_entry\n  DIAGNOSIS DATE -> diagnosis_date\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP601 UNIQUE ID -> cyp601_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  MAPPED SNOMED CT PREVIOUS DIAGNOSIS CODE -> mapped_snomed_ct_previous_diagnosis_code\n  MASTER SNOMED CT PREVIOUS DIAGNOSIS CODE -> master_snomed_ct_previous_diagnosis_code\n  MASTER SNOMED CT PREVIOUS DIAGNOSIS PREFERRED TERM -> master_snomed_ct_previous_diagnosis_preferred_term\n  MAPPED ICD-10 PREVIOUS DIAGNOSIS CODE -> mapped_icd_10_previous_diagnosis_code\n  MASTER ICD-10 PREVIOUS DIAGNOSIS CODE -> master_icd_10_previous_diagnosis_code\n  MASTER ICD-10 PREVIOUS DIAGNOSIS DESCRIPTION -> master_icd_10_previous_diagnosis_description\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "DIAGNOSIS SCHEME IN USE (COMMUNITY CARE)" as diagnosis_scheme_in_use_community_care,
    "DIAGNOSIS SCHEME IN USE" as diagnosis_scheme_in_use,
    "PREVIOUS DIAGNOSIS (CODED CLINICAL ENTRY)" as previous_diagnosis_coded_clinical_entry,
    "DIAGNOSIS DATE" as diagnosis_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP601 UNIQUE ID" as cyp601_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "MAPPED SNOMED CT PREVIOUS DIAGNOSIS CODE" as mapped_snomed_ct_previous_diagnosis_code,
    "MASTER SNOMED CT PREVIOUS DIAGNOSIS CODE" as master_snomed_ct_previous_diagnosis_code,
    "MASTER SNOMED CT PREVIOUS DIAGNOSIS PREFERRED TERM" as master_snomed_ct_previous_diagnosis_preferred_term,
    "MAPPED ICD-10 PREVIOUS DIAGNOSIS CODE" as mapped_icd_10_previous_diagnosis_code,
    "MASTER ICD-10 PREVIOUS DIAGNOSIS CODE" as master_icd_10_previous_diagnosis_code,
    "MASTER ICD-10 PREVIOUS DIAGNOSIS DESCRIPTION" as master_icd_10_previous_diagnosis_description,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP601MedicalHistory') }}
