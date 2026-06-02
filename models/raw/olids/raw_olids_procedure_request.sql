{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PROCEDURE_REQUEST \ndbt: source(''olids'', ''PROCEDURE_REQUEST'') \nColumns:\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ID -> id\n  PERSON_ID -> person_id\n  PATIENT_ID -> patient_id\n  ENCOUNTER_ID -> encounter_id\n  PRACTITIONER_ID -> practitioner_id\n  CLINICAL_EFFECTIVE_DATE -> clinical_effective_date\n  CLINICAL_EFFECTIVE_DATE_PRECISION_SOURCE_CONCEPT_ID -> clinical_effective_date_precision_source_concept_id\n  DATE_RECORDED -> date_recorded\n  DESCRIPTION -> description\n  PROCEDURE_REQUEST_SOURCE_CONCEPT_ID -> procedure_request_source_concept_id\n  STATUS_SOURCE_CONCEPT_ID -> status_source_concept_id\n  AGE_AT_EVENT -> age_at_event\n  AGE_AT_EVENT_BABY -> age_at_event_baby\n  AGE_AT_EVENT_NEONATE -> age_at_event_neonate\n  IS_CONFIDENTIAL -> is_confidential\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  PATIENT_SHARD_ID -> patient_shard_id\n  PERSON_SHARD_ID -> person_shard_id\n  LDS_SOURCE_RECORD_SHARD_ID -> lds_source_record_shard_id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_SOURCE_DATASET_ID -> lds_source_dataset_id\n  LDS_CDM_EVENT_ID -> lds_cdm_event_id\n  LDS_VERSIONER_EVENT_ID -> lds_versioner_event_id\n  LDS_DATETIME_FIRST_ACQUIRED -> lds_datetime_first_acquired\n  LDS_DATETIME_UPDATE_ACQUIRED -> lds_datetime_update_acquired\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_START_DATETIME -> lds_start_datetime\n  LDS_LAKEHOUSE_DATE_PROCESSED -> lds_lakehouse_date_processed\n  LDS_LAKEHOUSE_DATETIME_UPDATED -> lds_lakehouse_datetime_updated"
    )
}}
select
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ID" as id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "ENCOUNTER_ID" as encounter_id,
    "PRACTITIONER_ID" as practitioner_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "CLINICAL_EFFECTIVE_DATE_PRECISION_SOURCE_CONCEPT_ID" as clinical_effective_date_precision_source_concept_id,
    "DATE_RECORDED" as date_recorded,
    "DESCRIPTION" as description,
    "PROCEDURE_REQUEST_SOURCE_CONCEPT_ID" as procedure_request_source_concept_id,
    "STATUS_SOURCE_CONCEPT_ID" as status_source_concept_id,
    "AGE_AT_EVENT" as age_at_event,
    "AGE_AT_EVENT_BABY" as age_at_event_baby,
    "AGE_AT_EVENT_NEONATE" as age_at_event_neonate,
    "IS_CONFIDENTIAL" as is_confidential,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "PATIENT_SHARD_ID" as patient_shard_id,
    "PERSON_SHARD_ID" as person_shard_id,
    "LDS_SOURCE_RECORD_SHARD_ID" as lds_source_record_shard_id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_SOURCE_DATASET_ID" as lds_source_dataset_id,
    "LDS_CDM_EVENT_ID" as lds_cdm_event_id,
    "LDS_VERSIONER_EVENT_ID" as lds_versioner_event_id,
    "LDS_DATETIME_FIRST_ACQUIRED" as lds_datetime_first_acquired,
    "LDS_DATETIME_UPDATE_ACQUIRED" as lds_datetime_update_acquired,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_START_DATETIME" as lds_start_datetime,
    "LDS_LAKEHOUSE_DATE_PROCESSED" as lds_lakehouse_date_processed,
    "LDS_LAKEHOUSE_DATETIME_UPDATED" as lds_lakehouse_datetime_updated
from {{ source('olids', 'PROCEDURE_REQUEST') }}
