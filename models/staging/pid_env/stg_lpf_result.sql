{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'result']
    )
}}
-- Description: Staging layer (Providers submissions from PID environment via MESH). Cleaned and deduplicated data. \nSource: DATA_LAKE.LOCAL_PROVIDER_FLOWS.RESULT \ndbt: source(''local_provider_flows'', ''RESULT'') \nColumns:\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified\n  DeleteInd -> delete_ind\n  ResultID -> result_id\n  Version -> version\n  PersonID -> person_id\n  EncounterID -> encounter_id\n  ResultCodeID -> result_code_id\n  ResultCodeSystemID -> result_code_system_id\n  ResultDisplay -> result_display\n  ResultDate -> result_date\n  ResultValueNumber -> result_value_number\n  ResultValueNumberModifier -> result_value_number_modifier\n  UnitOfMeasureCodeID -> unit_of_measure_code_id\n  UnitOfMeasureCodeSystemID -> unit_of_measure_code_system_id\n  UnitOfMeasureDisplay -> unit_of_measure_display\n  ResultValueText -> result_value_text\n  ResultValueCodeID -> result_value_code_id\n  ResultValueCodeSystemID -> result_value_code_system_id\n  ResultValueCodeDisplay -> result_value_code_display\n  ResultValueDate -> result_value_date\n  ReferenceRange -> reference_range\n  InterpretationCodeID -> interpretation_code_id\n  InterpretationCodeSystemID -> interpretation_code_system_id\n  InterpretationDisplay -> interpretation_display\n  StatusCodeID -> status_code_id\n  StatusCodeSystemID -> status_code_system_id\n  StatusDisplay -> status_display\n  SpecimenTypeCodeID -> specimen_type_code_id\n  SpecimenTypeCodeSystemID -> specimen_type_code_system_id\n  SpecimenTypeDisplay -> specimen_type_display\n  MeasurementMethodCodeID -> measurement_method_code_id\n  MeasurementMethodCodeSystemID -> measurement_method_code_system_id\n  MeasurementMethodDisplay -> measurement_method_display\n  Accession -> accession\n  RecorderType -> recorder_type\n  DeviceModel -> device_model\n  DeviceSerialNumber -> device_serial_number\n  DeviceTypeCodeID -> device_type_code_id\n  DeviceTypeCodeSystemID -> device_type_code_system_id\n  DeviceTypeDisplay -> device_type_display\n  DeviceManufacturer -> device_manufacturer"
with raw_data as (
    select * from {{ ref('raw_lpf_result') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and result_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by result_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
