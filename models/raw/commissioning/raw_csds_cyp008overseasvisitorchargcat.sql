-- Raw layer model for csds.CYP008OverseasVisitorChargCat
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "OVERSEAS VISITOR CHARGING CATEGORY" as overseas_visitor_charging_category,
    "OVERSEAS VISITOR CHARGING CATEGORY APPLICABLE FROM DATE" as overseas_visitor_charging_category_applicable_from_date,
    "OVERSEAS VISITOR CHARGING CATEGORY APPLICABLE END DATE" as overseas_visitor_charging_category_applicable_end_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP008 UNIQUE ID" as cyp008_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "PERSON ID" as person_id,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDSCRO" as dmic_dscro,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP008OverseasVisitorChargCat') }}
