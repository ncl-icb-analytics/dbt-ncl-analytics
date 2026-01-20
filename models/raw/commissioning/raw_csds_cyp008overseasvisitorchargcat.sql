{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP008OverseasVisitorChargCat \ndbt: source(''csds'', ''CYP008OverseasVisitorChargCat'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  OVERSEAS VISITOR CHARGING CATEGORY -> overseas_visitor_charging_category\n  OVERSEAS VISITOR CHARGING CATEGORY APPLICABLE FROM DATE -> overseas_visitor_charging_category_applicable_from_date\n  OVERSEAS VISITOR CHARGING CATEGORY APPLICABLE END DATE -> overseas_visitor_charging_category_applicable_end_date\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP008 UNIQUE ID -> cyp008_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  PERSON ID -> person_id\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDSCRO -> dmic_dscro\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
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
