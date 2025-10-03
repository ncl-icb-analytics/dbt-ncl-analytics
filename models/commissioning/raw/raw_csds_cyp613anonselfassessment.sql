-- Raw layer model for csds.CYP613AnonSelfAssessment
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
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
