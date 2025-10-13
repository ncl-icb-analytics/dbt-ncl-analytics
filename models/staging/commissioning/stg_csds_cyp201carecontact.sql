{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
{{
    deduplicate_csds(
        csds_table = ref('raw_csds_cyp201carecontact'),
        partition_cols = ['unique_care_contact_identifier']
    )
}} )

select unique_service_request_identifier
    , person_id
    , unique_care_contact_identifier
    , care_contact_date
    , activity_location_type_code
    , attendance_status
from deduplicated