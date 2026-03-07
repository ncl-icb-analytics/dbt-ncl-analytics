{{ config(
    materialized='table',
    cluster_by=['person_id']) }}

-- HF_61 case finding dimension: patients with possible undiagnosed heart failure
-- Uses the HF-specific base population plus inline branch logic to match existing LTC LCS CF model layout

WITH base_population AS (
    SELECT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_hf_61_base') }}
),

branch_1_hf_specific_meds AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_branch_1_medication_date
    FROM {{ ref('int_ltc_lcs_hf_medications') }}
    WHERE valueset_friendly_name IN (
        'hf_case_finding_eligible_patients_vs1',
        'hf_case_finding_eligible_patients_vs2',
        'hf_case_finding_eligible_patients_vs3'
    )
        AND is_recent_3m = TRUE
    GROUP BY person_id
),

branch_2_sglt2_or_digoxin_group AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_branch_2_medication_date
    FROM {{ ref('int_ltc_lcs_hf_medications') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs4'
        AND is_recent_3m = TRUE
    GROUP BY person_id
),

ntprobnp_all AS (
    SELECT
        observation_id,
        person_id,
        clinical_effective_date AS ntprobnp_date,
        result_value AS ntprobnp_value
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs5'
),

cardiology_referral_latest AS (
    SELECT
        observation_id,
        person_id,
        clinical_effective_date AS referral_date
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs6'
),

hf_excluded_latest AS (
    -- EMIS ICB_CF_HF_61 excludes "Heart failure excluded" (vs7) within 3 years
    -- vs1 is Sacubitril/Valsartan medications, NOT HF excluded
    SELECT
        observation_id,
        person_id,
        clinical_effective_date AS hf_excluded_date
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs7'
        AND clinical_effective_date >= DATEADD(YEAR, -3, CURRENT_DATE())
),

rule_3_candidate_bnp AS (
    SELECT
        observation_id,
        person_id,
        ntprobnp_date,
        ntprobnp_value
    FROM ntprobnp_all
    WHERE ntprobnp_value > 400
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY ntprobnp_date DESC, observation_id DESC
    ) = 1
),

rule_4_candidate_bnp AS (
    SELECT
        observation_id,
        person_id,
        ntprobnp_date,
        ntprobnp_value
    FROM ntprobnp_all
    WHERE ntprobnp_date >= DATEADD(YEAR, -2, CURRENT_DATE())
        AND ntprobnp_value > 2000
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY ntprobnp_date DESC, observation_id DESC
    ) = 1
),

recent_hf_excluded AS (
    SELECT
        person_id,
        MAX(hf_excluded_date) AS latest_hf_excluded_date
    FROM hf_excluded_latest
    GROUP BY person_id
),

branch_3_ntprobnp_over_400 AS (
    SELECT
        bnp.person_id,
        bnp.ntprobnp_date AS latest_branch_3_ntprobnp_date,
        bnp.ntprobnp_value AS latest_branch_3_ntprobnp_value
    FROM rule_3_candidate_bnp AS bnp
    WHERE NOT EXISTS (
        SELECT 1
        FROM cardiology_referral_latest AS ref
        WHERE ref.person_id = bnp.person_id
            AND ref.referral_date >= bnp.ntprobnp_date
    )
),

branch_4_ntprobnp_over_2000 AS (
    SELECT
        bnp.person_id,
        bnp.ntprobnp_date AS latest_branch_4_ntprobnp_date,
        bnp.ntprobnp_value AS latest_branch_4_ntprobnp_value
    FROM rule_4_candidate_bnp AS bnp
    WHERE NOT EXISTS (
        SELECT 1
        FROM hf_excluded_latest AS ex
        WHERE ex.person_id = bnp.person_id
            AND ex.hf_excluded_date >= bnp.ntprobnp_date
    )
),

rule_5_group_a AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name IN (
        'hf_case_finding_eligible_patients_vs7',
        'hf_case_finding_eligible_patients_vs8'
    )
),

rule_5_group_b AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name IN (
        'hf_case_finding_eligible_patients_vs7',
        'hf_case_finding_eligible_patients_vs9'
    )
),

branch_5_cardiomyopathy_breathlessness AS (
    SELECT
        group_a.person_id
    FROM rule_5_group_a AS group_a
    INNER JOIN rule_5_group_b AS group_b
        ON group_a.person_id = group_b.person_id
),

branch_6_digoxin AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_branch_6_medication_date
    FROM {{ ref('int_ltc_lcs_hf_medications') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs10'
        AND is_recent_3m = TRUE
    GROUP BY person_id
),

female_patients AS (
    SELECT DISTINCT person_id
    FROM {{ ref('dim_person_gender') }}
    WHERE gender = 'Female'
),

hirsutism_codes AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name = 'hf_case_finding_eligible_patients_vs13'
),

pcos_pattern_exclusion AS (
    SELECT
        base.person_id
    FROM base_population AS base
    INNER JOIN female_patients AS female
        ON base.person_id = female.person_id
    INNER JOIN hirsutism_codes AS hirs
        ON base.person_id = hirs.person_id
    WHERE base.age < 40
),

branch_7_spironolactone AS (
    SELECT
        meds.person_id,
        MAX(meds.order_date) AS latest_branch_7_medication_date
    FROM {{ ref('int_ltc_lcs_hf_medications') }} AS meds
    LEFT JOIN pcos_pattern_exclusion AS pcos
        ON meds.person_id = pcos.person_id
    WHERE meds.valueset_friendly_name = 'hf_case_finding_eligible_patients_vs11'
        AND meds.is_recent_3m = TRUE
        AND pcos.person_id IS NULL
    GROUP BY meds.person_id
),

patient_rules AS (
    SELECT
        base.person_id,
        b1.latest_branch_1_medication_date,
        b2.latest_branch_2_medication_date,
        b3.latest_branch_3_ntprobnp_date,
        b3.latest_branch_3_ntprobnp_value,
        b4.latest_branch_4_ntprobnp_date,
        b4.latest_branch_4_ntprobnp_value,
        b6.latest_branch_6_medication_date,
        b7.latest_branch_7_medication_date,
        ex.latest_hf_excluded_date,
        (b1.person_id IS NOT NULL) AS branch_1_passed,
        (b2.person_id IS NOT NULL) AS branch_2_passed,
        (b3.person_id IS NOT NULL) AS branch_3_passed,
        (b4.person_id IS NOT NULL) AS branch_4_passed,
        (b5.person_id IS NOT NULL) AS branch_5_passed,
        (b6.person_id IS NOT NULL) AS branch_6_passed,
        (b7.person_id IS NOT NULL) AS branch_7_passed,
        (ex.person_id IS NOT NULL) AS has_recent_hf_excluded
    FROM base_population AS base
    LEFT JOIN branch_1_hf_specific_meds AS b1
        ON base.person_id = b1.person_id
    LEFT JOIN branch_2_sglt2_or_digoxin_group AS b2
        ON base.person_id = b2.person_id
    LEFT JOIN branch_3_ntprobnp_over_400 AS b3
        ON base.person_id = b3.person_id
    LEFT JOIN branch_4_ntprobnp_over_2000 AS b4
        ON base.person_id = b4.person_id
    LEFT JOIN branch_5_cardiomyopathy_breathlessness AS b5
        ON base.person_id = b5.person_id
    LEFT JOIN branch_6_digoxin AS b6
        ON base.person_id = b6.person_id
    LEFT JOIN branch_7_spironolactone AS b7
        ON base.person_id = b7.person_id
    LEFT JOIN recent_hf_excluded AS ex
        ON base.person_id = ex.person_id
)

SELECT
    person_id,
    latest_branch_1_medication_date,
    latest_branch_2_medication_date,
    latest_branch_3_ntprobnp_date,
    latest_branch_3_ntprobnp_value,
    latest_branch_4_ntprobnp_date,
    latest_branch_4_ntprobnp_value,
    latest_branch_6_medication_date,
    latest_branch_7_medication_date,
    latest_hf_excluded_date,
    branch_1_passed,
    branch_2_passed,
    branch_3_passed,
    branch_4_passed,
    branch_5_passed,
    branch_6_passed,
    branch_7_passed
FROM patient_rules
WHERE (
    branch_1_passed
    OR branch_2_passed
    OR branch_3_passed
    OR branch_4_passed
    OR branch_5_passed
    OR branch_6_passed
    OR branch_7_passed
)
    AND has_recent_hf_excluded = FALSE
