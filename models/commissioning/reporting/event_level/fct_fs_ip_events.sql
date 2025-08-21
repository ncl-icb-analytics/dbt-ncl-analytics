{{ config(materialized='table') }}

SELECT
    CURRENT_DATE() AS refresh_date,
    CAST(PRIMARYKEY_ID AS VARCHAR) AS primary_id,
    unique_id,
    CAST("spell.patient.identity.nhs_number.value Pseudo" AS INT) AS patient_id,
    'SUS-Faster' AS dataset,
    'IP' AS pod_group,
    "DETERMINE_POD__CH_TEMP"("spell.admission.method", "spell.admission.patient_classification", "spell.admission.date", "spell.discharge.date") AS pod,
    "DETERMINE_FISCAL_YEAR__CH_TEMP"("spell.discharge.date") AS fin_year,
    MONTHNAME("spell.discharge.date") AS fin_month_text,
    MOD(MONTH("spell.discharge.date") + 8, 12) + 1 AS fin_month,
    -- plus all your other calculated fields...
    *
FROM {{ ref('int_spell_enriched') }}
