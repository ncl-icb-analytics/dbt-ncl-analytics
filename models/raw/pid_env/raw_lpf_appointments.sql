{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.APPOINTMENTS \ndbt: source(''local_provider_flows'', ''APPOINTMENTS'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  AppointmentID -> appointment_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  StartDate -> start_date\n  EndDate -> end_date\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  TypeCodeSystemID -> type_code_system_id\n  TypeDisplay -> type_display\n  ReasonCodeID -> reason_code_id\n  ReasonCodeSystemID -> reason_code_system_id\n  ReasonDisplay -> reason_display\n  LocationID -> location_id\n  LocationDisplay -> location_display\n  TypeCodeID -> type_code_id"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "AppointmentID" as appointment_id,
    "Version" as version,
    "PersonID" as person_id,
    "EncounterID" as encounter_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "StatusCodeID" as status_code_id,
    "StatusCodeSystemID" as status_code_system_id,
    "StatusDisplay" as status_display,
    "TypeCodeSystemID" as type_code_system_id,
    "TypeDisplay" as type_display,
    "ReasonCodeID" as reason_code_id,
    "ReasonCodeSystemID" as reason_code_system_id,
    "ReasonDisplay" as reason_display,
    "LocationID" as location_id,
    "LocationDisplay" as location_display,
    "TypeCodeID" as type_code_id
from {{ source('local_provider_flows', 'APPOINTMENTS') }}
