/*
Flu Long-term Residential Care Eligibility Rule

Business Rule: Person is eligible if they have:
1. LONGRES_GROUP - a long-term residential care code (LONGRES_COD) that is no older than
   their latest residence code (RESIDE_COD), i.e. LONGRES_DAT >= RESIDE_DAT
2. AND aged 6 months or over (no upper age limit)

UKHSA removed the long-stay residential care indicator and the LONGRES_COD group from
the flu specification at v15.7, so no campaign from 2026-27 reports this cohort. The
campaigns that were reported with it keep their rows, driven by the
eligible_long_term_residential_care flag in flu_campaign_config().

LONGRES_COD is a subset of RESIDE_COD, so the two clusters are compared as separate
latest dates rather than ranked in a single union - ranking ties every qualifying
observation with itself and picks a winner nondeterministically.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Campaigns that still report the long-stay residential care cohort
    -- (campaign list: macros/config/flu_campaign_selection.sql)
    SELECT *
    FROM (
        {{ flu_reported_campaigns() }}
    )
    WHERE eligible_long_term_residential_care = TRUE
),

-- Step 1: Latest long-term care code date per person (LONGRES_DAT)
latest_longres_date AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS longres_date
    FROM ({{ get_observations("'LONGRES_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Latest residence code date per person (RESIDE_DAT)
latest_residence_date AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS residence_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 3: Long-term care code is current if not superseded by a later residence code
people_in_long_term_care AS (
    SELECT
        l.campaign_id,
        l.person_id,
        l.longres_date AS latest_residential_date
    FROM latest_longres_date l
    LEFT JOIN latest_residence_date r
        ON l.campaign_id = r.campaign_id
        AND l.person_id = r.person_id
    WHERE r.residence_date IS NULL
        OR l.longres_date >= r.residence_date
),

-- Step 4: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT
        pltc.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Long Term Residential Care' AS risk_group,
        pltc.person_id,
        pltc.latest_residential_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People living in care homes or long-term residential care' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_in_long_term_care pltc
    JOIN all_campaigns cc
        ON pltc.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON pltc.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or over (no upper age limit)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
