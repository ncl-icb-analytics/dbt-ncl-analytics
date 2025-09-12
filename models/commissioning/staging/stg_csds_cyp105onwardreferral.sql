-- Staging model for csds.CYP105OnwardReferral
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

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
