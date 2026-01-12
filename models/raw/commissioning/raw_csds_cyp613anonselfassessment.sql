{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP613AnonSelfAssessment \ndbt: source(''csds'', ''CYP613AnonSelfAssessment'') \nColumns:\n  ASSESSMENT TOOL COMPLETION DATE -> assessment_tool_completion_date\n  CODED ASSESSMENT TOOL TYPE (SNOMED CT) -> coded_assessment_tool_type_snomed_ct\n  PERSON SCORE -> person_score\n  ACTIVITY LOCATION TYPE CODE -> activity_location_type_code\n  ORGANISATION IDENTIFIER (CODE OF COMMISSIONER) -> organisation_identifier_code_of_commissioner\n  ORGANISATION CODE (CODE OF COMMISSIONER) -> organisation_code_code_of_commissioner\n  EFFECTIVE FROM -> effective_from\n  CYP613 UNIQUE ID -> cyp613_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "ASSESSMENT TOOL COMPLETION DATE" as assessment_tool_completion_date,
    "CODED ASSESSMENT TOOL TYPE (SNOMED CT)" as coded_assessment_tool_type_snomed_ct,
    "PERSON SCORE" as person_score,
    "ACTIVITY LOCATION TYPE CODE" as activity_location_type_code,
    "ORGANISATION IDENTIFIER (CODE OF COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "ORGANISATION CODE (CODE OF COMMISSIONER)" as organisation_code_code_of_commissioner,
    "EFFECTIVE FROM" as effective_from,
    "CYP613 UNIQUE ID" as cyp613_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP613AnonSelfAssessment') }}
