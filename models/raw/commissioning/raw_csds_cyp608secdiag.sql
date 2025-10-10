-- Raw layer model for csds.CYP608SecDiag
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "SERVICE REQUEST IDENTIFIER" as service_request_identifier,
    "DIAGNOSIS SCHEME IN USE (COMMUNITY CARE)" as diagnosis_scheme_in_use_community_care,
    "DIAGNOSIS SCHEME IN USE" as diagnosis_scheme_in_use,
    "SECONDARY DIAGNOSIS (CODED CLINICAL ENTRY)" as secondary_diagnosis_coded_clinical_entry,
    "DIAGNOSIS DATE" as diagnosis_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP608 UNIQUE ID" as cyp608_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE SERVICE REQUEST IDENTIFIER" as unique_service_request_identifier,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "MAPPED SNOMED CT SECONDARY DIAGNOSIS CODE" as mapped_snomed_ct_secondary_diagnosis_code,
    "MASTER SNOMED CT SECONDARY DIAGNOSIS CODE" as master_snomed_ct_secondary_diagnosis_code,
    "MASTER SNOMED CT SECONDARY DIAGNOSIS PREFERRED TERM" as master_snomed_ct_secondary_diagnosis_preferred_term,
    "MAPPED ICD-10 SECONDARY DIAGNOSIS CODE" as mapped_icd_10_secondary_diagnosis_code,
    "MASTER ICD-10 SECONDARY DIAGNOSIS CODE" as master_icd_10_secondary_diagnosis_code,
    "MASTER ICD-10 SECONDARY DIAGNOSIS DESCRIPTION" as master_icd_10_secondary_diagnosis_description,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP608SecDiag') }}
