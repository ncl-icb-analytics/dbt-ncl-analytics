{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.DIAGNOSIS \ndbt: source(''local_provider_flows'', ''DIAGNOSIS'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  DiagnosisID -> diagnosis_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  DiagnosisCodeID -> diagnosis_code_id\n  DiagnosisCodeSystemID -> diagnosis_code_system_id\n  DiagnosisDisplay -> diagnosis_display\n  DiagnosisDate -> diagnosis_date\n  ClassificationCodeID -> classification_code_id\n  ClassificationCodeSystemID -> classification_code_system_id\n  ClassificationDisplay -> classification_display\n  ConfirmationCodeID -> confirmation_code_id\n  ConfirmationCodeSystemID -> confirmation_code_system_id\n  ConfirmationDisplay -> confirmation_display\n  ProviderID -> provider_id\n  ProviderSurname -> provider_surname\n  ProviderGivenName -> provider_given_name\n  ProviderMiddleName -> provider_middle_name\n  ProviderFullName -> provider_full_name\n  BillingRank -> billing_rank\n  PresentOnAdmissionCodeID -> present_on_admission_code_id\n  PresentOnAdmissionCodeSystemID -> present_on_admission_code_system_id\n  PresentOnAdmissionDisplay -> present_on_admission_display\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  AssertedDate -> asserted_date"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "DiagnosisID" as diagnosis_id,
    "Version" as version,
    "PersonID" as person_id,
    "EncounterID" as encounter_id,
    "DiagnosisCodeID" as diagnosis_code_id,
    "DiagnosisCodeSystemID" as diagnosis_code_system_id,
    "DiagnosisDisplay" as diagnosis_display,
    "DiagnosisDate" as diagnosis_date,
    "ClassificationCodeID" as classification_code_id,
    "ClassificationCodeSystemID" as classification_code_system_id,
    "ClassificationDisplay" as classification_display,
    "ConfirmationCodeID" as confirmation_code_id,
    "ConfirmationCodeSystemID" as confirmation_code_system_id,
    "ConfirmationDisplay" as confirmation_display,
    "ProviderID" as provider_id,
    "ProviderSurname" as provider_surname,
    "ProviderGivenName" as provider_given_name,
    "ProviderMiddleName" as provider_middle_name,
    "ProviderFullName" as provider_full_name,
    "BillingRank" as billing_rank,
    "PresentOnAdmissionCodeID" as present_on_admission_code_id,
    "PresentOnAdmissionCodeSystemID" as present_on_admission_code_system_id,
    "PresentOnAdmissionDisplay" as present_on_admission_display,
    "StatusCodeID" as status_code_id,
    "StatusCodeSystemID" as status_code_system_id,
    "StatusDisplay" as status_display,
    "AssertedDate" as asserted_date
from {{ source('local_provider_flows', 'DIAGNOSIS') }}
