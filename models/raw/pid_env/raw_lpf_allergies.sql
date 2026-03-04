{{
    config(
        description="Raw layer (Providers submissions from PID environment via MESH). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.ALLERGIES \ndbt: source(''local_provider_flows'', ''ALLERGIES'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  AllergyID -> allergy_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  AllergenCodeID -> allergen_code_id\n  AllergenCodeSystemID -> allergen_code_system_id\n  AllergenDisplay -> allergen_display\n  OnsetDate -> onset_date\n  ResolvedDate -> resolved_date\n  ReactionCodeID -> reaction_code_id\n  ReactionCodeSystemID -> reaction_code_system_id\n  ReactionDisplay -> reaction_display\n  SeverityCodeID -> severity_code_id\n  SeverityCodeSystemID -> severity_code_system_id\n  SeverityDisplay -> severity_display\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  CategoryCodeID -> category_code_id\n  CategoryCodeSystemID -> category_code_system_id\n  CategoryDisplay -> category_display\n  TypeCodeID -> type_code_id\n  TypeCodeSystemID -> type_code_system_id\n  TypeDisplay -> type_display\n  CriticalityCodeID -> criticality_code_id\n  CriticalityCodeSystemID -> criticality_code_system_id\n  CriticalityDisplay -> criticality_display\n  AssertedDate -> asserted_date"
    )
}}
select
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified,
    "DeleteInd" as delete_ind,
    "AllergyID" as allergy_id,
    "Version" as version,
    "PersonID" as person_id,
    "EncounterID" as encounter_id,
    "AllergenCodeID" as allergen_code_id,
    "AllergenCodeSystemID" as allergen_code_system_id,
    "AllergenDisplay" as allergen_display,
    "OnsetDate" as onset_date,
    "ResolvedDate" as resolved_date,
    "ReactionCodeID" as reaction_code_id,
    "ReactionCodeSystemID" as reaction_code_system_id,
    "ReactionDisplay" as reaction_display,
    "SeverityCodeID" as severity_code_id,
    "SeverityCodeSystemID" as severity_code_system_id,
    "SeverityDisplay" as severity_display,
    "StatusCodeID" as status_code_id,
    "StatusCodeSystemID" as status_code_system_id,
    "StatusDisplay" as status_display,
    "CategoryCodeID" as category_code_id,
    "CategoryCodeSystemID" as category_code_system_id,
    "CategoryDisplay" as category_display,
    "TypeCodeID" as type_code_id,
    "TypeCodeSystemID" as type_code_system_id,
    "TypeDisplay" as type_display,
    "CriticalityCodeID" as criticality_code_id,
    "CriticalityCodeSystemID" as criticality_code_system_id,
    "CriticalityDisplay" as criticality_display,
    "AssertedDate" as asserted_date
from {{ source('local_provider_flows', 'ALLERGIES') }}
