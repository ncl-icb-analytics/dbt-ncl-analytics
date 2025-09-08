{{
    config(
        materialized='view')
}}


/*
Recent outpatient activities from SUS

Processing:
- build marts

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

with total_attendances_12mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_att_tot_12mo
    FROM {{ ref('int_sus_op_min') }}
    where appointment_attended_or_dna in ('5', '6') -- Attended
    GROUP BY sk_patient_id
),
total_appointments_12mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_app_tot_12mo
    FROM {{ ref('int_sus_op_min') }}
    GROUP BY sk_patient_id
),
count_of_specialties as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT primary_reason_for_event) AS op_spec_12mo
    FROM {{ ref('int_sus_op_min') }}
    where primary_reason_for_event is not null
    GROUP BY sk_patient_id
),
count_of_providers as(
      SELECT
        sk_patient_id
        ,COUNT(DISTINCT(provider_id)) AS op_prov_12mo
    FROM {{ ref('int_sus_op_min') }}
    where provider_id is not null
    GROUP BY sk_patient_id
)

SELECT
    a.sk_patient_id
    ,op_att_tot_12mo
    ,op_app_tot_12mo
    ,op_spec_12mo
    ,op_prov_12mo
FROM total_attendances_12mo as a
left join total_appointments_12mo as b on a.sk_patient_id = b.sk_patient_id
left join count_of_specialties as c on a.sk_patient_id = c.sk_patient_id
left join count_of_providers as d on a.sk_patient_id = d.sk_patient_id
