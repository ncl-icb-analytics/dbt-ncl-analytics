-- Raw layer model for csds.CYP612CodedAssessmentContact
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "CARE ACTIVITY IDENTIFIER" as care_activity_identifier,
    "CODED ASSESSMENT TOOL TYPE (SNOMED CT)" as coded_assessment_tool_type_snomed_ct,
    "PERSON SCORE" as person_score,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP612 UNIQUE ID" as cyp612_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE CARE ACTIVITY IDENTIFIER" as unique_care_activity_identifier,
    "AGE AT  ASSESSMENT TOOL (CONTACT) COMPLETION DATE (DAYS)" as age_at_assessment_tool_contact_completion_date_days,
    "IC_AGE_AT_ASSESSMENT_TOOL_CONTACT_COMPLETION_DATE" as ic_age_at_assessment_tool_contact_completion_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "ASQ SCORE BAND" as asq_score_band,
    "SNOMED CT ASSESSMENT PREFERRED TERM" as snomed_ct_assessment_preferred_term,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicObservationDate" as dmic_observation_date,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP612CodedAssessmentContact') }}
