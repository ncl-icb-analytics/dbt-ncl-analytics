-- Staging model for csds.CYP901StaffDetails
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

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
