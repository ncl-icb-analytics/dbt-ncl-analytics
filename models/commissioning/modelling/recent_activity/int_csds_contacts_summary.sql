{{
    config(
        materialized='table')
}}

/*
A summary of the number and types of contacts per referral

Grain: referral (service_request_identifier)

Clinical Purpose:
- understanding utilisation across community care

*/

{% set years_from_now = -1 %}

SELECT 

    referral.service_request_identifier AS referral_id,
    bridging.pseudo_nhs_number AS sk_patient_id,

    -- count care contacts
    COUNT(contact.care_contact_identifier) AS contact_count,
    COUNT_IF(contact.attendance_status IN ('5', '6')) AS count_seen,
    COUNT_IF(contact.attendance_status IN ('7', '3')) AS count_dna,
    COUNT_IF(contact.attendance_status = '2') AS count_patient_cancelled,
    COUNT_IF(contact.attendance_status = '4') AS count_clinician_cancelled,

    -- calculate average duration
    AVG(contact.clinical_contact_duration_of_care_contact) AS average_duration,

    -- count care contact location types - clinical, patient related, other
    COUNT_IF(contact.activity_location_type_code ILIKE ANY ('B', 'C', 'D', 'E', 'N04', 'N05')) AS count_location_clinical,
    COUNT_IF(contact.activity_location_type_code ILIKE ANY ('A', 'G' )) AS count_location_home,
    COUNT_IF(contact.activity_location_type_code ILIKE ANY ('F', 'H', 'J', 'K', 'L', 'M', 'N01', 'N02', 'N03', 'X01')) AS count_location_other

FROM
    {{ ref('int_csds_cyp101referral_dedup') }} AS referral
LEFT JOIN
    {{ ref('int_csds_cyp201carecontact_dedup') }} AS contact
ON 
    referral.service_request_identifier = contact.service_request_identifier
LEFT JOIN
    {{ ref('stg_csds_bridging') }} AS bridging 

ON 
        referral.person_id = bridging.person_id

WHERE referral.referral_request_received_date >= DATEADD(YEAR, {{years_from_now}}, current_date())
GROUP BY
    referral.service_request_identifier,
    bridging.pseudo_nhs_number