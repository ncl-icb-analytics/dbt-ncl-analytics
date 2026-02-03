{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.IMMUNISATION \ndbt: source(''local_provider_flows'', ''IMMUNISATION'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  ImmunisationID -> immunisation_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  ImmunisationCodeID -> immunisation_code_id\n  ImmunisationCodeSystemID -> immunisation_code_system_id\n  ImmunisationDisplay -> immunisation_display\n  ImmunisationDate -> immunisation_date\n  DrugCodeID -> drug_code_id\n  DrugCodeSystemID -> drug_code_system_id\n  DrugDisplay -> drug_display\n  DoseAmount -> dose_amount\n  DoseUnitCodeID -> dose_unit_code_id\n  DoseUnitCodeSystemID -> dose_unit_code_system_id\n  DoseUnitDisplay -> dose_unit_display\n  RouteCodeID -> route_code_id\n  RouteCodeSystemID -> route_code_system_id\n  RouteDisplay -> route_display\n  RefusalInd -> refusal_ind\n  RefusalReasonCodeID -> refusal_reason_code_id\n  RefusalReasonCodeSystemID -> refusal_reason_code_system_id\n  RefusalReasonDisplay -> refusal_reason_display\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  AdministeringProviderID -> administering_provider_id\n  AdministeringProviderSurname -> administering_provider_surname\n  AdministeringProviderGivenName -> administering_provider_given_name\n  AdministeringProviderMiddleName -> administering_provider_middle_name\n  AdministeringProviderFullName -> administering_provider_full_name"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "ImmunisationID" as immunisation_id,
    "Version" as version,
    "PersonID" as person_id,
    "EncounterID" as encounter_id,
    "ImmunisationCodeID" as immunisation_code_id,
    "ImmunisationCodeSystemID" as immunisation_code_system_id,
    "ImmunisationDisplay" as immunisation_display,
    "ImmunisationDate" as immunisation_date,
    "DrugCodeID" as drug_code_id,
    "DrugCodeSystemID" as drug_code_system_id,
    "DrugDisplay" as drug_display,
    "DoseAmount" as dose_amount,
    "DoseUnitCodeID" as dose_unit_code_id,
    "DoseUnitCodeSystemID" as dose_unit_code_system_id,
    "DoseUnitDisplay" as dose_unit_display,
    "RouteCodeID" as route_code_id,
    "RouteCodeSystemID" as route_code_system_id,
    "RouteDisplay" as route_display,
    "RefusalInd" as refusal_ind,
    "RefusalReasonCodeID" as refusal_reason_code_id,
    "RefusalReasonCodeSystemID" as refusal_reason_code_system_id,
    "RefusalReasonDisplay" as refusal_reason_display,
    "StatusCodeID" as status_code_id,
    "StatusCodeSystemID" as status_code_system_id,
    "StatusDisplay" as status_display,
    "AdministeringProviderID" as administering_provider_id,
    "AdministeringProviderSurname" as administering_provider_surname,
    "AdministeringProviderGivenName" as administering_provider_given_name,
    "AdministeringProviderMiddleName" as administering_provider_middle_name,
    "AdministeringProviderFullName" as administering_provider_full_name
from {{ source('local_provider_flows', 'IMMUNISATION') }}
