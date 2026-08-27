{{ config(materialized='table') }}

/*
Under-65 Clinical At-Risk Groups
for Covid Autumn 2024 booster campaign only people at risk under 65 were vaccinated.
This model retains the ten UKHSA ATRISK_GROUP clinical groups for people
under 65 at the campaign reference date. It preserves each clinical group;
it does not create a synthetic "Under 65 At Risk" parent row.
*/

{{ config(materialized='table') }}

WITH clinical_risk_groups AS (
    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_chronic_heart_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_chronic_liver_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_chronic_neurological_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, 'Asplenia' AS risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_asplenia') }}

    UNION ALL

    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_chronic_kidney_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_diabetes') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_immunosuppression') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_chronic_respiratory_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_severe_mental_illness') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_covid_learning_disability') }}
),

under_65_clinical_risk_groups AS (
    SELECT *
    FROM clinical_risk_groups
    WHERE birth_date_approx > DATEADD('year', -65, reference_date)
)

SELECT * FROM under_65_clinical_risk_groups
WHERE campaign_id <> 'COVID Spring 2025'
ORDER BY campaign_id, person_id, risk_group
