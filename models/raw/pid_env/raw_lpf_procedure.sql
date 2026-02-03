{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.PROCEDURE \ndbt: source(''local_provider_flows'', ''PROCEDURE'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  ProcedureID -> procedure_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  ProcedureCodeID -> procedure_code_id\n  ProcedureCodeSystemID -> procedure_code_system_id\n  ProcedureDisplay -> procedure_display\n  StartDate -> start_date\n  EndDate -> end_date\n  BillingRank -> billing_rank\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  ProviderID -> provider_id\n  ProviderSurname -> provider_surname\n  ProviderSurame -> provider_surame\n  ProviderGivenName -> provider_given_name\n  ProviderMiddleName -> provider_middle_name\n  ProviderFullName -> provider_full_name\n  ProviderRoleCodeID -> provider_role_code_id\n  ProviderRoleCodeSystemID -> provider_role_code_system_id\n  ProviderRoleDisplay -> provider_role_display"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "ProcedureID" as procedure_id,
    "Version" as version,
    "PersonID" as person_id,
    "EncounterID" as encounter_id,
    "ProcedureCodeID" as procedure_code_id,
    "ProcedureCodeSystemID" as procedure_code_system_id,
    "ProcedureDisplay" as procedure_display,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "BillingRank" as billing_rank,
    "StatusCodeID" as status_code_id,
    "StatusCodeSystemID" as status_code_system_id,
    "StatusDisplay" as status_display,
    "ProviderID" as provider_id,
    "ProviderSurname" as provider_surname,
    "ProviderSurame" as provider_surame,
    "ProviderGivenName" as provider_given_name,
    "ProviderMiddleName" as provider_middle_name,
    "ProviderFullName" as provider_full_name,
    "ProviderRoleCodeID" as provider_role_code_id,
    "ProviderRoleCodeSystemID" as provider_role_code_system_id,
    "ProviderRoleDisplay" as provider_role_display
from {{ source('local_provider_flows', 'PROCEDURE') }}
