{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.REFERRAL_REQUEST \ndbt: source(''local_provider_flows'', ''REFERRAL_REQUEST'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  ReferralRequestID -> referral_request_id\n  Version -> version\n  PersonID -> person_id\n  ReferralReasonCodeID -> referral_reason_code_id\n  ReferralReasonCodeSystemID -> referral_reason_code_system_id\n  ReferralReasonDisplay -> referral_reason_display\n  SentDate -> sent_date\n  PrincipalDiagnosisCodeID -> principal_diagnosis_code_id\n  PrincipalDiagnosisCodeSystemID -> principal_diagnosis_code_system_id\n  PrincipalDiagnosisDisplay -> principal_diagnosis_display\n  PrincipalDiagnosisDate -> principal_diagnosis_date\n  EstimatedDateOfBirth -> estimated_date_of_birth\n  RequestingProviderID -> requesting_provider_id\n  RequestingProviderSurname -> requesting_provider_surname\n  RequestingProviderGivenName -> requesting_provider_given_name\n  RequestingProviderMiddleName -> requesting_provider_middle_name\n  RequestingProviderFullName -> requesting_provider_full_name\n  ReferralToProviderID -> referral_to_provider_id\n  ReferralToProviderSurname -> referral_to_provider_surname\n  ReferralToProviderGivenName -> referral_to_provider_given_name\n  ReferralToProviderMiddleName -> referral_to_provider_middle_name\n  ReferralToProviderFullName -> referral_to_provider_full_name"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "ReferralRequestID" as referral_request_id,
    "Version" as version,
    "PersonID" as person_id,
    "ReferralReasonCodeID" as referral_reason_code_id,
    "ReferralReasonCodeSystemID" as referral_reason_code_system_id,
    "ReferralReasonDisplay" as referral_reason_display,
    "SentDate" as sent_date,
    "PrincipalDiagnosisCodeID" as principal_diagnosis_code_id,
    "PrincipalDiagnosisCodeSystemID" as principal_diagnosis_code_system_id,
    "PrincipalDiagnosisDisplay" as principal_diagnosis_display,
    "PrincipalDiagnosisDate" as principal_diagnosis_date,
    "EstimatedDateOfBirth" as estimated_date_of_birth,
    "RequestingProviderID" as requesting_provider_id,
    "RequestingProviderSurname" as requesting_provider_surname,
    "RequestingProviderGivenName" as requesting_provider_given_name,
    "RequestingProviderMiddleName" as requesting_provider_middle_name,
    "RequestingProviderFullName" as requesting_provider_full_name,
    "ReferralToProviderID" as referral_to_provider_id,
    "ReferralToProviderSurname" as referral_to_provider_surname,
    "ReferralToProviderGivenName" as referral_to_provider_given_name,
    "ReferralToProviderMiddleName" as referral_to_provider_middle_name,
    "ReferralToProviderFullName" as referral_to_provider_full_name
from {{ source('local_provider_flows', 'REFERRAL_REQUEST') }}
