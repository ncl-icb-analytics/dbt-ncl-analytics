{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: PUBLISHED_REPORTING__DIRECT_CARE.C_LTCS.CLTCS_INCLUSION_COHORT_ARCHIVE \ndbt: source(''c_ltcs'', ''CLTCS_INCLUSION_COHORT_ARCHIVE'') \nColumns:\n  PATIENT_ID -> patient_id\n  AREA_CODE -> area_code\n  PRACTICE_COD -> practice_cod\n  COHORT_EVENT -> cohort_event\n  IS_ACTIVE -> is_active\n  EVENT_WRITTEN_AT -> event_written_at"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "AREA_CODE" as area_code,
    "PRACTICE_COD" as practice_cod,
    "COHORT_EVENT" as cohort_event,
    "IS_ACTIVE" as is_active,
    "EVENT_WRITTEN_AT" as event_written_at
from {{ source('c_ltcs', 'CLTCS_INCLUSION_COHORT_ARCHIVE') }}
