-- Staging model for sus_apc.spell.episodes.births
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "BIRTHS_ID" as births_id,
    "birth_order" as birth_order,
    "delivery_method" as delivery_method,
    "gestation_length_assessment" as gestation_length_assessment,
    "resuscitation_method" as resuscitation_method,
    "status_of_person_conducting_delivery" as status_of_person_conducting_delivery,
    "delivery_place.type" as delivery_place_type,
    "live_or_still_birth" as live_or_still_birth,
    "birth_weight" as birth_weight,
    "baby.gender" as baby_gender,
    "baby.local_patient_identifier.value" as baby_local_patient_identifier_value,
    "baby.local_patient_identifier.issuer" as baby_local_patient_identifier_issuer,
    "baby.nhs_number.value Pseudo" as baby_nhs_number_value_pseudo,
    "baby.nhs_number.status" as baby_nhs_number_status,
    "baby.birth_date" as baby_birth_date,
    "delivery_place.location_class" as delivery_place_location_class,
    "delivery_place.location_type" as delivery_place_location_type,
    "baby.withheld_identity_reason" as baby_withheld_identity_reason,
    "baby.overseas_visitor_status" as baby_overseas_visitor_status,
    "dmicImportLogId" as dmicimportlogid,
    "baby.overseas_visitor_charging_category_at_cds_activity_date" as baby_overseas_visitor_charging_category_at_cds_activity_date
from {{ source('sus_apc', 'spell.episodes.births') }}
