-- Staging model for csds.CYP609CodedAssessmentReferral
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

select
    "SK" as sk,
    "SERVICE REQUEST IDENTIFIER" as service_request_identifier,
    "CODED ASSESSMENT TOOL TYPE (SNOMED CT)" as coded_assessment_tool_type_snomed_ct,
    "PERSON SCORE" as person_score,
    "ASSESSMENT TOOL COMPLETION DATE" as assessment_tool_completion_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP609 UNIQUE ID" as cyp609_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE SERVICE REQUEST IDENTIFIER" as unique_service_request_identifier,
    "AGE AT ASSESSMENT TOOL (REFERRAL) COMPLETION DATE (DAYS)" as age_at_assessment_tool_referral_completion_date_days,
    "AGE AT ASSESSMENT TOOL (REFERRAL) COMPLETION DATE (YEARS)" as age_at_assessment_tool_referral_completion_date_years,
    "IC_AGE_AT_ASSESSMENT_TOLL_REFERRAL_COMPLETION_DATE" as ic_age_at_assessment_toll_referral_completion_date,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "AGE GROUP (ASSESSMENT COMPLETION DATE)" as age_group_assessment_completion_date,
    "AGE BAND (ASSESSMENT COMPLETION DATE)" as age_band_assessment_completion_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP609CodedAssessmentReferral') }}
