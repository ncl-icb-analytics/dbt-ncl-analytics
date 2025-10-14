{{
    config(materialized = 'view')
}}

WITH deduplicated AS (
{{
    deduplicate_csds(
        csds_table = ref('raw_csds_cyp202careactivity'),
        partition_cols = ['unique_care_activity_identifier']
    )
}} )


select unique_care_activity_identifier
    , unique_care_contact_identifier
    , community_care_activity_type
    , person_id
    , clinical_contact_duration_of_care_activity
    , coded_procedure_clinical_terminology
    , coded_finding_coded_clinical_entry
    , coded_observation_clinical_terminology
    , effective_from
from deduplicated