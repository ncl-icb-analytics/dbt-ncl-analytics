-- Staging model for csds_simple.tblCare_Activity
-- Source: "DATA_LAKE"."CSDS_SIMPLE"
-- Description: Community services dataset (simple)

select
    "Unique_care_contact_identifier" as unique_care_contact_identifier,
    "Unique_care_activity_identifier" as unique_care_activity_identifier,
    "Community_care_activity_type" as community_care_activity_type,
    "Person_ID" as person_id,
    "Care_professional_local_identifier" as care_professional_local_identifier,
    "Clinical_contact_duration_of_care_activity" as clinical_contact_duration_of_care_activity,
    "NHSNumber_Pseudo" as nhs_number_pseudo,
    "Breast_Feeding_Status_(Latest)_DV" as breast_feeding_status_latest_dv,
    "Person_Weight_(Latest)_DV" as person_weight_latest_dv,
    "Person_Height_in_metres_(Latest)_DV" as person_height_in_metres_latest_dv,
    "Person_Length_in_centimetres_(Latest)_DV" as person_length_in_centimetres_latest_dv,
    "Count_of_coded_findings_DV" as count_of_coded_findings_dv,
    "Count_of_coded_observations_DV" as count_of_coded_observations_dv,
    "Count_of_coded_procedures_DV" as count_of_coded_procedures_dv
from {{ source('csds_simple', 'tblCare_Activity') }}
