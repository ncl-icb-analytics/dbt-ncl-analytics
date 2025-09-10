-- Staging model for csds.CYP605IPE
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "INFANT PHYSICAL EXAMINATION DATE" as infant_physical_examination_date,
    "INFANT PHYSICAL EXAMINATION RESULT (HIPS)" as infant_physical_examination_result_hips,
    "INFANT PHYSICAL EXAMINATION RESULT (HEART)" as infant_physical_examination_result_heart,
    "INFANT PHYSICAL EXAMINATION RESULT (EYES)" as infant_physical_examination_result_eyes,
    "INFANT PHYSICAL EXAMINATION RESULT (TESTES)" as infant_physical_examination_result_testes,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP605 UNIQUE ID" as cyp605_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "AGE AT 6-8 WEEK PHYSICAL EXAMINATION" as age_at_6_8_week_physical_examination,
    "IC_AGE_AT_6_8_WEEK_PHYSICAL_EXAMINATION" as ic_age_at_6_8_week_physical_examination,
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
from {{ source('csds', 'CYP605IPE') }}
