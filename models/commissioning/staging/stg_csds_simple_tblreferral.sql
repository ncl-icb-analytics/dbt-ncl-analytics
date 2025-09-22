-- Staging model for csds_simple.tblReferral
-- Source: "DATA_LAKE"."CSDS_SIMPLE"
-- Description: Community services dataset (simple)

select
    "Person_ID" as person_id,
    "Pseudo_NHS_Number" as pseudo_nhs_number,
    "Unique_service_request_identifier" as unique_service_request_identifier,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "Record_Number" as record_number,
    "Organisation_identifier_(Code_of_provider)" as organisation_identifier_code_of_provider,
    "Organisation_identifier_(Code_of_commissioner)" as organisation_identifier_code_of_commissioner,
    "Organisation_code_(Code_of_commissioner)" as organisation_code_code_of_commissioner,
    "Referring_organisation_code" as referring_organisation_code,
    "Referral_request_received_date" as referral_request_received_date,
    "Referral_request_received_time" as referral_request_received_time,
    "Referring_care_professional_staff_group_(Community_care)" as referring_care_professional_staff_group_community_care,
    "Service_discharge_date" as service_discharge_date,
    "Discharge_letter_issued_date_(Community_care)" as discharge_letter_issued_date_community_care,
    "Source_of_referral_for_community" as source_of_referral_for_community,
    "Priority_type_code" as priority_type_code,
    "Primary_reason_for_referral_(Community_care)" as primary_reason_for_referral_community_care,
    "Age_at_service_received_date" as age_at_service_received_date,
    "Age_at_service_referral_discharge" as age_at_service_referral_discharge,
    "ServTeamtoCommunityCare_(List)_DV" as serv_teamto_community_care_list_dv,
    "ServOrTeamTypeRefToCC_(Latest_List)_DV" as serv_or_team_type_ref_to_cc_latest_list_dv,
    "Organisation_identifier_(Patient_pathway_identifier_issuer)" as organisation_identifier_patient_pathway_identifier_issuer,
    "Waiting_time_measurement_type_(Community_Care)" as waiting_time_measurement_type_community_care,
    "Referral_to_treatment_period_start_date" as referral_to_treatment_period_start_date,
    "Referaal_to_treatment_period_start_time" as referaal_to_treatment_period_start_time,
    "Referral_to_treatment_period_end_date" as referral_to_treatment_period_end_date,
    "Referral_to_treatment_period_end_time" as referral_to_treatment_period_end_time,
    "Referral_to_treatment_period_status" as referral_to_treatment_period_status,
    "Onward_referral_date" as onward_referral_date,
    "Onward_referral_reason_(Community_care)" as onward_referral_reason_community_care,
    "Onward_Referral_reason" as onward_referral_reason,
    "Organisation_identifier_(Receiving_organisation)" as organisation_identifier_receiving_organisation
from {{ source('csds_simple', 'tblReferral') }}
