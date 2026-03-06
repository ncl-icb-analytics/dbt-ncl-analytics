{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS HF observations
-- Collects all HF case-finding observation evidence using LTC LCS valueset references

WITH hf_observations AS (
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('eligible_for_hf_casefinding_vs1') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs5') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs6') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs7') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs8') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs9') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs13') }})
    UNION ALL
    SELECT * FROM ({{ get_ltc_lcs_observations_latest('hf_case_finding_eligible_patients_vs1') }})
)

SELECT
    *,
    CASE
        WHEN valueset_friendly_name = 'eligible_for_hf_casefinding_vs1'
            THEN 'base_hf_evidence'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs5'
            THEN 'branch_3_4_ntprobnp'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs6'
            THEN 'branch_3_cardiology_referral'
        WHEN valueset_friendly_name IN (
            'hf_case_finding_eligible_patients_vs7',
            'hf_case_finding_eligible_patients_vs1'
        ) THEN 'hf_excluded'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs8'
            THEN 'branch_5_cardiomyopathy'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs9'
            THEN 'branch_5_breathlessness'
        WHEN valueset_friendly_name = 'hf_case_finding_eligible_patients_vs13'
            THEN 'branch_7_hirsutism'
        ELSE 'unmapped'
    END AS observation_branch
FROM hf_observations
