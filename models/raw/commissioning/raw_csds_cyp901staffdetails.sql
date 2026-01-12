{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP901StaffDetails \ndbt: source(''csds'', ''CYP901StaffDetails'') \nColumns:\n  CARE PROFESSIONAL LOCAL IDENTIFIER -> care_professional_local_identifier\n  PROFESSIONAL REGISTRATION BODY CODE -> professional_registration_body_code\n  PROFESSIONAL REGISTRATION ENTRY IDENTIFIER -> professional_registration_entry_identifier\n  CARE PROFESSIONAL STAFF GROUP (COMMUNITY CARE) -> care_professional_staff_group_community_care\n  OCCUPATION CODE -> occupation_code\n  CARE PROFESSIONAL (JOB ROLE CODE) -> care_professional_job_role_code\n  EFFECTIVE FROM -> effective_from\n  CYP901 UNIQUE ID -> cyp901_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_CareProfessionalId_Local -> unique_care_professional_id_local\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "CARE PROFESSIONAL LOCAL IDENTIFIER" as care_professional_local_identifier,
    "PROFESSIONAL REGISTRATION BODY CODE" as professional_registration_body_code,
    "PROFESSIONAL REGISTRATION ENTRY IDENTIFIER" as professional_registration_entry_identifier,
    "CARE PROFESSIONAL STAFF GROUP (COMMUNITY CARE)" as care_professional_staff_group_community_care,
    "OCCUPATION CODE" as occupation_code,
    "CARE PROFESSIONAL (JOB ROLE CODE)" as care_professional_job_role_code,
    "EFFECTIVE FROM" as effective_from,
    "CYP901 UNIQUE ID" as cyp901_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_CareProfessionalId_Local" as unique_care_professional_id_local,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP901StaffDetails') }}
