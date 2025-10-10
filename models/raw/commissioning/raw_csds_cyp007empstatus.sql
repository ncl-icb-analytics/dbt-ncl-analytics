-- Raw layer model for csds.CYP007EmpStatus
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "EMPLOYMENT STATUS" as employment_status,
    "EMPLOYMENT STATUS RECORDED DATE" as employment_status_recorded_date,
    "WEEKLY HOURS WORKED" as weekly_hours_worked,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP007 UNIQUE ID" as cyp007_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "PERSON ID" as person_id,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP007EmpStatus') }}
