{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.births \ndbt: source(''sus_apc'', ''spell.episodes.births'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  BIRTHS_ID -> births_id\n  birth_order -> birth_order\n  delivery_method -> delivery_method\n  gestation_length_assessment -> gestation_length_assessment\n  resuscitation_method -> resuscitation_method\n  status_of_person_conducting_delivery -> status_of_person_conducting_delivery\n  delivery_place.type -> delivery_place_type\n  live_or_still_birth -> live_or_still_birth\n  birth_weight -> birth_weight\n  baby.gender -> baby_gender\n  baby.local_patient_identifier.value -> baby_local_patient_identifier_value\n  baby.local_patient_identifier.issuer -> baby_local_patient_identifier_issuer\n  baby.nhs_number.value Pseudo -> baby_nhs_number_value_pseudo\n  baby.nhs_number.status -> baby_nhs_number_status\n  baby.birth_date -> baby_birth_date\n  delivery_place.location_class -> delivery_place_location_class\n  delivery_place.location_type -> delivery_place_location_type\n  baby.withheld_identity_reason -> baby_withheld_identity_reason\n  baby.overseas_visitor_status -> baby_overseas_visitor_status\n  dmicImportLogId -> dmic_import_log_id\n  baby.overseas_visitor_charging_category_at_cds_activity_date -> baby_overseas_visitor_charging_category_at_cds_activity_date"
    )
}}
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
    "dmicImportLogId" as dmic_import_log_id,
    "baby.overseas_visitor_charging_category_at_cds_activity_date" as baby_overseas_visitor_charging_category_at_cds_activity_date
from {{ source('sus_apc', 'spell.episodes.births') }}
