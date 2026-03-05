{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

{#
    Person Status Summary Dimension
    ================================
    
    Wide dimension table combining all person status flags into a single model
    with one row per person. Enables easy joining to semantic views and
    population analysis.
    
    Grain: One row per person (from dim_person_demographics)
    
    All status flags default to FALSE for persons without records in the
    underlying status tables, ensuring complete population coverage.
#}

WITH all_persons AS (
    SELECT person_id
    FROM {{ ref('dim_person_demographics') }}
),

-- Care Home Status
care_home AS (
    SELECT
        person_id,
        is_care_home_resident,
        is_nursing_home_resident,
        is_temporary_resident,
        residence_type
    FROM {{ ref('dim_person_care_home') }}
),

-- Homeless Status (presence indicates homeless or registered at CHIP)
homeless AS (
    SELECT
        person_id,
        TRUE AS is_homeless_or_chip,
        CASE 
            WHEN registered_chip = TRUE THEN TRUE 
            ELSE FALSE 
        END AS is_registered_chip,
        CASE 
            WHEN concept_code IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_homeless_code
    FROM {{ ref('dim_person_homeless') }}
),

-- Carer Status
carer AS (
    SELECT
        person_id,
        is_carer,
        carer_type
    FROM {{ ref('dim_person_is_carer') }}
),

-- Housebound Status
housebound AS (
    SELECT
        person_id,
        is_housebound,
        housebound_status
    FROM {{ ref('dim_person_housebound_status') }}
),

-- Polypharmacy Status
polypharmacy AS (
    SELECT
        person_id,
        medication_count,
        is_polypharmacy_5plus,
        is_polypharmacy_10plus,
        medication_count_band
    FROM {{ ref('fct_person_polypharmacy_current') }}
),

-- Alcohol Status
alcohol AS (
    SELECT
        person_id,
        alcohol_status,
        alcohol_risk_sort_key,
        requires_intervention AS alcohol_requires_intervention
    FROM {{ ref('fct_person_alcohol_status') }}
),

-- Smoking Status
smoking AS (
    SELECT
        person_id,
        smoking_status,
        smoking_risk_sort_key
    FROM {{ ref('fct_person_smoking_status') }}
),

-- Pregnancy Status (only currently pregnant)
pregnancy AS (
    SELECT
        person_id,
        is_currently_pregnant
    FROM {{ ref('fct_person_pregnancy_status') }}
),

-- Type 1 Opt-out Status
opt_out AS (
    SELECT
        person_id,
        is_opted_out AS is_type1_opted_out
    FROM {{ ref('dim_person_opt_out_type_1_status') }}
),

-- Secondary Use Allowed
secondary_use AS (
    SELECT
        person_id,
        is_allowed_secondary_use
    FROM {{ ref('dim_person_secondary_use_allowed') }}
),

-- Looked After Child
looked_after AS (
    SELECT
        person_id,
        TRUE AS is_looked_after_child
    FROM {{ ref('dim_looked_after_child') }}
)

SELECT
    p.person_id,
    
    -- Care Home Status
    COALESCE(ch.is_care_home_resident, FALSE) AS is_care_home_resident,
    COALESCE(ch.is_nursing_home_resident, FALSE) AS is_nursing_home_resident,
    COALESCE(ch.is_temporary_resident, FALSE) AS is_temporary_care_home,
    ch.residence_type AS care_home_type,
    
    -- Homeless Status
    COALESCE(hm.is_homeless_or_chip, FALSE) AS is_homeless_or_chip,
    COALESCE(hm.has_homeless_code, FALSE) AS has_homeless_code,
    COALESCE(hm.is_registered_chip, FALSE) AS is_registered_chip,
    
    -- Carer Status
    COALESCE(cr.is_carer, FALSE) AS is_carer,
    cr.carer_type,
    
    -- Housebound Status
    COALESCE(hb.is_housebound, FALSE) AS is_housebound,
    hb.housebound_status,
    
    -- Polypharmacy Status
    COALESCE(poly.medication_count, 0) AS medication_count,
    COALESCE(poly.is_polypharmacy_5plus, FALSE) AS is_polypharmacy_5plus,
    COALESCE(poly.is_polypharmacy_10plus, FALSE) AS is_polypharmacy_10plus,
    COALESCE(poly.medication_count_band, '0') AS medication_count_band,
    
    -- Alcohol Status
    COALESCE(alc.alcohol_status, 'Unknown') AS alcohol_status,
    COALESCE(alc.alcohol_risk_sort_key, 0) AS alcohol_risk_sort_key,
    COALESCE(alc.alcohol_requires_intervention, FALSE) AS alcohol_requires_intervention,
    
    -- Smoking Status
    COALESCE(smk.smoking_status, 'Unknown') AS smoking_status,
    COALESCE(smk.smoking_risk_sort_key, 0) AS smoking_risk_sort_key,
    
    -- Pregnancy Status
    COALESCE(preg.is_currently_pregnant, FALSE) AS is_currently_pregnant,
    
    -- Data Sharing Status
    COALESCE(opt.is_type1_opted_out, FALSE) AS is_type1_opted_out,
    COALESCE(su.is_allowed_secondary_use, TRUE) AS is_allowed_secondary_use,
    
    -- Looked After Child
    COALESCE(lac.is_looked_after_child, FALSE) AS is_looked_after_child,
    
    -- Vulnerability Summary Flags
    CASE
        WHEN COALESCE(ch.is_care_home_resident, FALSE)
            OR COALESCE(hm.is_homeless_or_chip, FALSE)
            OR COALESCE(hb.is_housebound, FALSE)
            OR COALESCE(lac.is_looked_after_child, FALSE)
        THEN TRUE
        ELSE FALSE
    END AS has_vulnerability_flag,
    
    -- Risk Factor Summary
    (
        CASE WHEN COALESCE(smk.smoking_status, 'Unknown') = 'Current Smoker' THEN 1 ELSE 0 END +
        CASE WHEN COALESCE(alc.alcohol_risk_sort_key, 0) >= 5 THEN 1 ELSE 0 END +  -- Higher risk or worse
        CASE WHEN COALESCE(poly.is_polypharmacy_10plus, FALSE) THEN 1 ELSE 0 END
    ) AS behavioural_risk_count

FROM all_persons p
LEFT JOIN care_home ch ON p.person_id = ch.person_id
LEFT JOIN homeless hm ON p.person_id = hm.person_id
LEFT JOIN carer cr ON p.person_id = cr.person_id
LEFT JOIN housebound hb ON p.person_id = hb.person_id
LEFT JOIN polypharmacy poly ON p.person_id = poly.person_id
LEFT JOIN alcohol alc ON p.person_id = alc.person_id
LEFT JOIN smoking smk ON p.person_id = smk.person_id
LEFT JOIN pregnancy preg ON p.person_id = preg.person_id
LEFT JOIN opt_out opt ON p.person_id = opt.person_id
LEFT JOIN secondary_use su ON p.person_id = su.person_id
LEFT JOIN looked_after lac ON p.person_id = lac.person_id
