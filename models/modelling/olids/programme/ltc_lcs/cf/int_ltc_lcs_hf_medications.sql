{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS HF medications
-- Collects all HF case-finding medication issues using LTC LCS valueset references

WITH hf_medications AS (
    SELECT * FROM ({{ get_ltc_lcs_medication_orders('hf_case_finding_eligible_patients_vs1') }})
    UNION ALL
    SELECT
        medication_order_id,
        medication_statement_id,
        patient_id,
        person_id,
        sk_patient_id,
        order_date,
        order_medication_name,
        order_dose,
        order_quantity_value,
        order_quantity_unit,
        order_duration_days,
        estimated_cost,
        statement_medication_name,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display,
        'ENTRESTO_PRODUCTS' AS valueset_id,
        'hf_case_finding_eligible_patients_vs2' AS valueset_friendly_name,
        bnf_code,
        bnf_name,
        'mapped' AS match_path
    FROM ({{ get_medication_orders(
        cluster_id="'ENTRESTO_PRODUCTS'",
        source="LTC_LCS"
    ) }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_medication_orders('hf_case_finding_eligible_patients_vs3') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_medication_orders('hf_case_finding_eligible_patients_vs4') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_medication_orders('hf_case_finding_eligible_patients_vs10') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_medication_orders('hf_case_finding_eligible_patients_vs11') }})
)

SELECT
    *,
    CASE
        WHEN order_date >= DATEADD(MONTH, -3, CURRENT_DATE()) THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,
    CASE
        WHEN valueset_friendly_name IN (
            'hf_case_finding_eligible_patients_vs1',
            'hf_case_finding_eligible_patients_vs2',
            'hf_case_finding_eligible_patients_vs3'
        ) THEN 'branch_1_hf_specific_meds'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs4'
            THEN 'branch_2_sglt2_or_digoxin_group'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs10'
            THEN 'branch_6_digoxin'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs11'
            THEN 'branch_7_spironolactone'
        ELSE 'unmapped'
    END AS medication_branch
FROM hf_medications
