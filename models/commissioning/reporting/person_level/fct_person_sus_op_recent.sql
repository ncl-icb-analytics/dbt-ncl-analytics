{{
    config(
        materialized='view')
}}


/*
Recent outpatient activities from SUS

Processing:
- build marts for recent (1year) total activity (unfiltered)

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
    FROM {{ ref('int_sus_op_1year') }}
    where appointment_attended_or_dna in ('5', '6') -- Attended
    GROUP BY sk_patient_id
),
total_attendances_3mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_att_tot_3mo
    FROM {{ ref('int_sus_op_1year') }}
    where appointment_attended_or_dna in ('5', '6') -- Attended
    and start_date between dateadd(month, -3, current_date()) and current_date()
    GROUP BY sk_patient_id
), 
total_attendances_1mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_att_tot_1mo
    FROM {{ ref('int_sus_op_1year') }}
    where appointment_attended_or_dna in ('5', '6') -- Attended
    and start_date between dateadd(month, -1, current_date()) and current_date()
    GROUP BY sk_patient_id
),
total_appointments_12mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_app_tot_12mo
    FROM {{ ref('int_sus_op_1year') }}
    GROUP BY sk_patient_id
),
first_attendances_12mo as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT event_id) AS op_att_first_12mo
    FROM {{ ref('int_sus_op_1year') }}
    where appointment_attended_or_dna in ('5', '6') -- Attended
    AND appointment_first_attendance IN ('1', '3') -- First Appointment
    GROUP BY sk_patient_id
),
count_of_specialties as(
    SELECT
        sk_patient_id
        ,COUNT(DISTINCT primary_reason_for_event) AS op_spec_12mo
    FROM {{ ref('int_sus_op_1year') }}
    where primary_reason_for_event is not null
    GROUP BY sk_patient_id
),
count_of_providers as(
      SELECT
        sk_patient_id
        ,COUNT(DISTINCT(provider_id)) AS op_prov_12mo
    FROM {{ ref('int_sus_op_1year') }}
    where provider_id is not null
    GROUP BY sk_patient_id
),
count_of_prov_per_spec as(
    SELECT
        sk_patient_id
        ,primary_reason_for_event
        ,COUNT(DISTINCT provider_id) AS op_prov_per_spec_12mo
    FROM {{ ref('int_sus_op_1year') }}
    where primary_reason_for_event is not null and provider_id is not null
    GROUP BY sk_patient_id, primary_reason_for_event
),
potential_dup_provider as(
      SELECT
        sk_patient_id
        ,COUNT(DISTINCT(primary_reason_for_event)) AS op_num_spec_2_prov_12mo
    FROM count_of_prov_per_spec
    where op_prov_per_spec_12mo > 1
    GROUP BY sk_patient_id
)

SELECT
    a.sk_patient_id
    ,ZEROIFNULL(op_att_tot_12mo) AS op_att_tot_12mo
    ,ZEROIFNULL(op_att_tot_3mo) AS op_att_tot_3mo
    ,ZEROIFNULL(op_att_tot_1mo) AS op_att_tot_1mo
    ,ZEROIFNULL(op_att_first_12mo) AS op_att_first_12mo
    ,ZEROIFNULL(op_app_tot_12mo) AS op_app_tot_12mo
    ,ZEROIFNULL(op_spec_12mo) AS op_spec_12mo
    ,ZEROIFNULL(op_prov_12mo) AS op_prov_12mo
    ,ZEROIFNULL(op_num_spec_2_prov_12mo) AS op_num_spec_2_prov_12mo
FROM total_attendances_12mo as a

left join total_appointments_12mo as b on a.sk_patient_id = b.sk_patient_id
left join count_of_specialties as c on a.sk_patient_id = c.sk_patient_id
left join count_of_providers as d on a.sk_patient_id = d.sk_patient_id
left join first_attendances_12mo as e on a.sk_patient_id = e.sk_patient_id
left join potential_dup_provider as f on a.sk_patient_id = f.sk_patient_id
left join total_attendances_3mo as g on a.sk_patient_id = g.sk_patient_id
left join total_attendances_1mo as h on a.sk_patient_id = h.sk_patient_id

