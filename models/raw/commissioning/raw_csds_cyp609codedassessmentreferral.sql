{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP609CodedAssessmentReferral \ndbt: source(''csds'', ''CYP609CodedAssessmentReferral'') \nColumns:\n  SK -> sk\n  SERVICE REQUEST IDENTIFIER -> service_request_identifier\n  CODED ASSESSMENT TOOL TYPE (SNOMED CT) -> coded_assessment_tool_type_snomed_ct\n  PERSON SCORE -> person_score\n  ASSESSMENT TOOL COMPLETION DATE -> assessment_tool_completion_date\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP609 UNIQUE ID -> cyp609_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE SERVICE REQUEST IDENTIFIER -> unique_service_request_identifier\n  AGE AT ASSESSMENT TOOL (REFERRAL) COMPLETION DATE (DAYS) -> age_at_assessment_tool_referral_completion_date_days\n  AGE AT ASSESSMENT TOOL (REFERRAL) COMPLETION DATE (YEARS) -> age_at_assessment_tool_referral_completion_date_years\n  IC_AGE_AT_ASSESSMENT_TOLL_REFERRAL_COMPLETION_DATE -> ic_age_at_assessment_toll_referral_completion_date\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  AGE GROUP (ASSESSMENT COMPLETION DATE) -> age_group_assessment_completion_date\n  AGE BAND (ASSESSMENT COMPLETION DATE) -> age_band_assessment_completion_date\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
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
