-- Staging model for csds_simple.tblDiagnosis
-- Source: "DATA_LAKE"."CSDS_SIMPLE"
-- Description: Community services dataset (simple)

select
    "Unique_service_request_identifier" as unique_service_request_identifier,
    "Code" as code,
    "Date" as date,
    "Diagnosis_Type" as diagnosis_type,
    "Diagnosis_scheme_in_use_(Community_care)" as diagnosis_scheme_in_use_community_care
from {{ source('csds_simple', 'tblDiagnosis') }}
