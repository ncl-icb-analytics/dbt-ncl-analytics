/*
COVID Vaccination Eligibility Fact Table

This model determines who is ELIGIBLE for COVID vaccination using clear, 
individual rule models instead of complex macros.

Key improvements:
- Each rule is implemented in its own clear model
- Business logic is explicit and documented
- Terminology is descriptive  
- Single configuration point for dates
- Direct use of core macros (get_observations, get_medication_orders)
- Works with multiple campaigns via covid_campaign_config macro
- Separate from vaccination status tracking (see fct_covid_status)

Multi-Campaign Support:
- COVID Autumn 2024: September 2024 - March 2025 (broader eligibility)
- COVID Spring 2025: April 2025 - June 2025 (restricted eligibility)
- COVID Autumn 2025: September 2025 - March 2026 (restricted eligibility)
- COVID Spring 2026: April 2026 - June 2026 (restricted eligibility)

Usage: 
- Default: Uses all defined COVID campaigns automatically
- Specific campaign analysis: Filter by campaign_id in downstream models
- For vaccination tracking, use fct_covid_status instead
- This replaces all the old complex macro-based models
- KH tidied eligible groups

*/

{{ config(
    materialized='table',
    tags=['covid_flu'],
    cluster_by=['campaign_id', 'person_id', 'campaign_category']
) }}

WITH
-- Age-based eligibility (all campaigns automatically included from intermediate models)
age_based_eligibility AS (
    -- Age 75 Plus (universal eligibility) - also include 65+ for the autumn 2024 campaign
    SELECT 
        campaign_id, 'age_based' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_age_75_plus') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_age_75_plus
),

-- Simple clinical condition eligibility for Under 65 at Risk
clinical_condition_eligibility AS (
 -- under_65_at_risk
    SELECT 
        campaign_id, 'clinical_condition' AScampaign_category, 'Under 65 at risk' as risk_group , risk_group as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_under_65_at_risk') }}
     --FROM MODELLING.OLIDS_PROGRAMME.int_covid_under_65_at_risk
     WHERE campaign_id = 'COVID Autumn 2024'
     
     UNION ALL
     
-- Immunosuppression under 75 for Spring 2025 and later campaigns
    SELECT 
        campaign_id, 'clinical_condition' AScampaign_category, 'Immunosuppressed under 75' AS risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_immunosuppression') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_immunosuppression
    WHERE campaign_id <> 'COVID Autumn 2024' and age_years_at_ref_date < 75
    ),

-- Other non clinical risk eligibility for Under 65 at Risk
other_risk_eligibility AS ( 
   -- Homeless (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_homeless') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_homeless 
    WHERE campaign_id = 'COVID Autumn 2024'
    
    UNION ALL
    
    -- Pregnancy (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_pregnancy') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_pregnancy
    
    UNION ALL

     -- Long-term Residential Care (hierarchical rule) age 18+ for Autumn 2024
    SELECT 
        campaign_id, 'Other Risk Group' AScampaign_category, 'Long Term Residential Care 18+' as risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_long_term_residential_care') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_long_term_residential_care
    WHERE campaign_id = 'COVID Autumn 2024'

    UNION ALL

     -- Long-term Residential Care (hierarchical rule) age 65+ for Spring 2025 and later campaigns
    SELECT 
        campaign_id, 'Other Risk Group' AScampaign_category, 'Long Term Residential Care 65+' as risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_long_term_residential_care') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_covid_long_term_residential_care
    WHERE campaign_id <> 'COVID Autumn 2024'
      
    UNION ALL

     -- Morbid Obesity (hierarchical rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_morbid_obesity') }}
     --FROM MODELLING.OLIDS_PROGRAMME.int_covid_morbid_obesity
     WHERE campaign_id = 'COVID Autumn 2024'
),

-- Union all eligibility types (vaccination tracking removed - belongs in separate table)
all_eligibility AS (
    SELECT * FROM age_based_eligibility
    UNION ALL
    SELECT * FROM clinical_condition_eligibility  
    UNION ALL
    SELECT * FROM other_risk_eligibility
),

-- Final formatting (campaign information already included in intermediate models)
final_eligibility AS (
    SELECT 
        campaign_id,
        campaign_category,
        risk_group,
        subcohort,
        person_id,
        qualifying_event_date,
        reference_date,
        description AS eligibility_reason,
        rule_type,
        eligibility_priority,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        created_at
    FROM all_eligibility
)

SELECT distinct * FROM final_eligibility
ORDER BY person_id, eligibility_priority, campaign_category