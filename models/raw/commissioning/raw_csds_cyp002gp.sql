{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP002GP \ndbt: source(''csds'', ''CYP002GP'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  GENERAL MEDICAL PRACTICE (PATIENT REGISTRATION) -> general_medical_practice_patient_registration\n  GENERAL MEDICAL PRACTICE CODE (PATIENT REGISTRATION) -> general_medical_practice_code_patient_registration\n  START DATE (GMP PATIENT REGISTRATION) -> start_date_gmp_patient_registration\n  END DATE (GMP PATIENT REGISTRATION) -> end_date_gmp_patient_registration\n  ORGANISATION IDENTIFIER (GP PRACTICE RESPONSIBILITY) -> organisation_identifier_gp_practice_responsibility\n  ORGANISATION CODE (GP PRACTICE RESPONSIBILITY) -> organisation_code_gp_practice_responsibility\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP002 UNIQUE ID -> cyp002_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  GP DISTANCE FROM HOME -> gp_distance_from_home\n  ORGANISATION IDENTIFIER (CCG OF GP PRACTICE) -> organisation_identifier_ccg_of_gp_practice\n  ORGANISATION CODE (CCG OF GP PRACTICE) -> organisation_code_ccg_of_gp_practice\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  ORGANISATION IDENTIFIER (SUB ICB LOCATION OF GP PRACTICE) -> organisation_identifier_sub_icb_location_of_gp_practice\n  ORGANISATION IDENTIFIER (ICB OF GP PRACTICE) -> organisation_identifier_icb_of_gp_practice\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  dmIcbRegistrationSubmitted -> dm_icb_registration_submitted\n  dmSubIcbRegistrationSubmitted -> dm_sub_icb_registration_submitted\n  dmCommissionerDerivationReason -> dm_commissioner_derivation_reason\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "GENERAL MEDICAL PRACTICE (PATIENT REGISTRATION)" as general_medical_practice_patient_registration,
    "GENERAL MEDICAL PRACTICE CODE (PATIENT REGISTRATION)" as general_medical_practice_code_patient_registration,
    "START DATE (GMP PATIENT REGISTRATION)" as start_date_gmp_patient_registration,
    "END DATE (GMP PATIENT REGISTRATION)" as end_date_gmp_patient_registration,
    "ORGANISATION IDENTIFIER (GP PRACTICE RESPONSIBILITY)" as organisation_identifier_gp_practice_responsibility,
    "ORGANISATION CODE (GP PRACTICE RESPONSIBILITY)" as organisation_code_gp_practice_responsibility,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP002 UNIQUE ID" as cyp002_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "GP DISTANCE FROM HOME" as gp_distance_from_home,
    "ORGANISATION IDENTIFIER (CCG OF GP PRACTICE)" as organisation_identifier_ccg_of_gp_practice,
    "ORGANISATION CODE (CCG OF GP PRACTICE)" as organisation_code_ccg_of_gp_practice,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "ORGANISATION IDENTIFIER (SUB ICB LOCATION OF GP PRACTICE)" as organisation_identifier_sub_icb_location_of_gp_practice,
    "ORGANISATION IDENTIFIER (ICB OF GP PRACTICE)" as organisation_identifier_icb_of_gp_practice,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "dmIcbRegistrationSubmitted" as dm_icb_registration_submitted,
    "dmSubIcbRegistrationSubmitted" as dm_sub_icb_registration_submitted,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP002GP') }}
