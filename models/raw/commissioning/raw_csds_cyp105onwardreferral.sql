{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP105OnwardReferral \ndbt: source(''csds'', ''CYP105OnwardReferral'') \nColumns:\n  SK -> sk\n  SERVICE REQUEST IDENTIFIER -> service_request_identifier\n  ONWARD REFERRAL DATE -> onward_referral_date\n  ONWARD REFERRAL REASON (COMMUNITY CARE) -> onward_referral_reason_community_care\n  ONWARD REFERRAL REASON -> onward_referral_reason\n  ORGANISATION IDENTIFIER (RECEIVING ORGANISATION) -> organisation_identifier_receiving_organisation\n  ORGANISATION IDENTIFIER (RECEIVING) -> organisation_identifier_receiving\n  ORGANISATION CODE (RECEIVING) -> organisation_code_receiving\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP105 UNIQUE ID -> cyp105_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE SERVICE REQUEST IDENTIFIER -> unique_service_request_identifier\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "SERVICE REQUEST IDENTIFIER" as service_request_identifier,
    "ONWARD REFERRAL DATE" as onward_referral_date,
    "ONWARD REFERRAL REASON (COMMUNITY CARE)" as onward_referral_reason_community_care,
    "ONWARD REFERRAL REASON" as onward_referral_reason,
    "ORGANISATION IDENTIFIER (RECEIVING ORGANISATION)" as organisation_identifier_receiving_organisation,
    "ORGANISATION IDENTIFIER (RECEIVING)" as organisation_identifier_receiving,
    "ORGANISATION CODE (RECEIVING)" as organisation_code_receiving,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP105 UNIQUE ID" as cyp105_unique_id,
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
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP105OnwardReferral') }}
