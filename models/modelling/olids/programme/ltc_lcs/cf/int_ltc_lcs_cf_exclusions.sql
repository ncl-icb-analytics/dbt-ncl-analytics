-- Intermediate model for LTC LCS Case Finding Exclusions
-- Identifies patients with conditions that exclude them from LTC LCS Case Finding.
-- Mirrors EMIS ICS_METABOLIC_LTC: AF, CKD, CHD, HTN, HF, Stroke/TIA, PAD, NAFLD, Diabetes (all types), Hyperlipidaemia.
-- Respiratory conditions (COPD, Asthma, CYP Asthma) are tracked separately as ICS_RESP_LTC equivalents
-- and NOT included in the shared has_metabolic_excluding_condition flag.

WITH ltc_summary_conditions AS (
    SELECT
        person_id,
        condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date

    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE condition_code IN (
        'CKD',
        'AF',
        'COPD',
        'HTN',
        'CHD',
        'STIA',
        'PAD',
        'HF',
        'FHYP',
        'NAF',
        'AST',
        'CYP_AST'
    )
    AND is_on_register = TRUE
),

diabetes_all AS (
    -- EMIS ICS_METABOLIC_LTC excludes ALL diabetes types (Type 1, Type 2, gestational, etc.)
    -- not just Type 2. Scoped to current register members to match other conditions.
    SELECT DISTINCT
        person_id,
        MIN(earliest_diagnosis_date) AS earliest_diagnosis_date,
        MAX(latest_diagnosis_date) AS latest_diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE
    GROUP BY person_id
),

all_conditions AS (
    SELECT * FROM ltc_summary_conditions
    UNION ALL
    SELECT
        person_id,
        'DM' AS condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM diabetes_all
),

person_level_aggregation AS (
    SELECT
        person_id,
        MIN(earliest_diagnosis_date) AS earliest_excluding_condition_date,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code = 'AF') AS has_af,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stia,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        BOOLOR_AGG(condition_code = 'HF') AS has_hf,
        BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
        BOOLOR_AGG(condition_code = 'FHYP') AS has_hyperlipidaemia,
        BOOLOR_AGG(condition_code = 'NAF') AS has_nafld,
        BOOLOR_AGG(condition_code = 'AST') AS has_asthma,
        BOOLOR_AGG(condition_code = 'CYP_AST') AS has_cyp_asthma
    FROM all_conditions
    GROUP BY person_id
)

SELECT
    person_id,
    has_ckd,
    has_af,
    has_copd,
    has_hypertension,
    has_chd,
    has_stia,
    has_pad,
    has_hf,
    has_diabetes,
    has_hyperlipidaemia,
    has_nafld,
    has_asthma,
    has_cyp_asthma,
    earliest_excluding_condition_date,

    -- ICS_METABOLIC_LTC equivalent: metabolic/cardiovascular conditions only
    -- Used by DM, CKD, CVD, HTN, AF, HF case finding base populations
    (
        has_ckd OR has_af OR has_hypertension OR has_chd
        OR has_stia OR has_pad OR has_hf OR has_diabetes
        OR has_hyperlipidaemia OR has_nafld
    ) AS has_metabolic_excluding_condition,

    -- ICS_RESP_LTC equivalent: respiratory conditions only
    -- Used by CYP Asthma case finding base population
    (has_copd OR has_asthma OR has_cyp_asthma) AS has_respiratory_excluding_condition,

    -- Legacy combined flag (kept for backward compatibility — prefer specific flags above)
    (
        has_ckd OR has_af OR has_copd OR has_hypertension OR has_chd
        OR has_stia OR has_pad OR has_hf OR has_diabetes
        OR has_hyperlipidaemia OR has_nafld OR has_asthma OR has_cyp_asthma
    ) AS has_excluding_condition
FROM person_level_aggregation
