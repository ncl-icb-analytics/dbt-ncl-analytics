{{
    config(
        materialized='table')
}}

/*
A summary of the number and types of activities per contact

Grain: contact (care_contact_identifier)

Clinical Purpose:
- understanding utilisation across community care

*/

{% set years_from_now = -1 %}

SELECT

    bridging.pseudo_nhs_number AS sk_patient_id,
    contact.service_request_identifier as referral_id,
    contact.care_contact_identifier AS contact_id,

    -- count activities
    COUNT(DISTINCT activity.care_activity_identifier) AS total_activities_count,
    COUNT_IF(activity.community_care_activity_type = '01') AS count_tests,
    COUNT_IF(activity.community_care_activity_type = '02') AS count_assessments,
    COUNT_IF(activity.community_care_activity_type = '03') AS count_clinical_interventions,
    COUNT_IF(activity.community_care_activity_type = '04') AS count_advice,
    COUNT_IF(activity.community_care_activity_type = '05') AS patient_health_promotion,
    COUNT_IF(activity.community_care_activity_type = '06') AS count_mdt_review,
    COUNT_IF(activity.community_care_activity_type = '07') AS count_clinician_support,
    COUNT_IF(activity.community_care_activity_type IN ('08','09','10','11','12')) AS count_cyp_health_visitor,
     
FROM
    {{ ref('int_csds_cyp201carecontact_dedup') }} AS contact
LEFT JOIN
    {{ ref('int_csds_cyp202careactivity_dedup') }} AS activity
ON 
    contact.care_contact_identifier = activity.care_contact_identifier

LEFT JOIN
    {{ ref('int_csds_cyp101referral_dedup')}} AS referral
ON 
    contact.service_request_identifier = referral.service_request_identifier

LEFT JOIN
    {{ ref('stg_csds_bridging') }} AS bridging 
ON 
        contact.person_id = bridging.person_id

WHERE referral.referral_request_received_date >= DATEADD(YEAR, {{years_from_now}}, current_date())

GROUP BY
    contact.care_contact_identifier,
    bridging.pseudo_nhs_number,
    contact.service_request_identifier