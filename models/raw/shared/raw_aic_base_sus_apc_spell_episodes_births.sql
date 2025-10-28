-- Raw layer model for aic.BASE_SUS__APC_SPELL_EPISODES_BIRTHS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "BIRTHS_ID" as births_id,
    "BIRTH_ORDER" as birth_order,
    "DELIVERY_METHOD" as delivery_method,
    "GESTATION_LENGTH_ASSESSMENT" as gestation_length_assessment,
    "RESUSCITATION_METHOD" as resuscitation_method,
    "STATUS_OF_PERSON_CONDUCTING_DELIVERY" as status_of_person_conducting_delivery,
    "DELIVERY_PLACE_TYPE" as delivery_place_type,
    "LIVE_OR_STILL_BIRTH" as live_or_still_birth,
    "BIRTH_WEIGHT" as birth_weight,
    "BABY_GENDER" as baby_gender,
    "BABY_LOCAL_PATIENT_IDENTIFIER_VALUE" as baby_local_patient_identifier_value,
    "BABY_LOCAL_PATIENT_IDENTIFIER_ISSUER" as baby_local_patient_identifier_issuer,
    "BABY_NHS_NUMBER_VALUE_PSEUDO" as baby_nhs_number_value_pseudo,
    "BABY_NHS_NUMBER_STATUS" as baby_nhs_number_status,
    "BABY_BIRTH_DATE" as baby_birth_date,
    "DELIVERY_PLACE_LOCATION_CLASS" as delivery_place_location_class,
    "DELIVERY_PLACE_LOCATION_TYPE" as delivery_place_location_type,
    "BABY_WITHHELD_IDENTITY_REASON" as baby_withheld_identity_reason,
    "BABY_OVERSEAS_VISITOR_STATUS" as baby_overseas_visitor_status,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "BABY_OVERSEAS_VISITOR_CHARGING_CATEGORY_AT_CDS_ACTIVITY_DATE" as baby_overseas_visitor_charging_category_at_cds_activity_date
from {{ source('aic', 'BASE_SUS__APC_SPELL_EPISODES_BIRTHS') }}
