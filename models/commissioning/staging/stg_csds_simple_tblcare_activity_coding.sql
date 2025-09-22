-- Staging model for csds_simple.tblCare_Activity_Coding
-- Source: "DATA_LAKE"."CSDS_SIMPLE"
-- Description: Community services dataset (simple)

select
    "Unique_service_request_identifier" as unique_service_request_identifier,
    "Unique_care_contact_identifier" as unique_care_contact_identifier,
    "Unique_care_activity_identifier" as unique_care_activity_identifier,
    "Code" as code,
    "Scheme_in_use_(Community_care)" as scheme_in_use_community_care,
    "Care_activity_type" as care_activity_type,
    "Value" as value,
    "UCUM_unit_of_measurement" as ucum_unit_of_measurement
from {{ source('csds_simple', 'tblCare_Activity_Coding') }}
