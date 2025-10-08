{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/
with total_count as(
    SELECT
        sk_patient_id
        ,COUNT(*) AS ers_total_count
    FROM {{ ref('int_ers_referrals_new') }}
    GROUP BY sk_patient_id
),
accepted_count as(
    SELECT
        sk_patient_id
        ,COUNT(*) AS ers_accepted_count
    FROM {{ ref('int_ers_referrals_new') }}
    where provider_org_name is not null
    GROUP BY sk_patient_id
),
count_by_specialty as(
    SELECT
        sk_patient_id
        ,specialty_desc
        ,COUNT(*) AS spec_total_count
    FROM {{ ref('int_ers_referrals_new') }}
    GROUP BY sk_patient_id, specialty_desc
),
number_of_specialty as(
      SELECT
        sk_patient_id
        ,COUNT(*) AS ers_specialty_count
    FROM count_by_specialty
    where specialty_desc is not null
    GROUP BY sk_patient_id
),

count_by_prov as(
    SELECT
        sk_patient_id
        ,PROVIDER_ORG_ID
        ,COUNT(*) AS prov_total_count
    FROM {{ ref('int_ers_referrals_new') }}
    GROUP BY sk_patient_id, PROVIDER_ORG_ID
),

number_of_providers as(
      SELECT
        sk_patient_id
        ,COUNT(*) AS ers_provider_count
    FROM count_by_prov
    where PROVIDER_ORG_ID is not null
    GROUP BY sk_patient_id
)

SELECT
    a.sk_patient_id
    ,ers_total_count
    ,ers_accepted_count
    ,ers_specialty_count
    ,ers_provider_count
FROM total_count as a
left join accepted_count as b on a.sk_patient_id = b.sk_patient_id
left join number_of_specialty as c on a.sk_patient_id = c.sk_patient_id
left join number_of_providers as d on a.sk_patient_id = d.sk_patient_id
