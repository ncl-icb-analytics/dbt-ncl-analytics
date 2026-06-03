{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All diabetes medication orders including insulins, antidiabetic drugs, and hypoglycaemia treatments.
Uses BNF classification (6.1.x) with detailed medication type categorisation.
GLP-1 receptor agonists are broken out of BNF 6.1.2.3 (Other antidiabetic drugs) using
int_glp1_medications_all, which categorises them to active ingredient via SNOMED clusters.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_orders AS (
    {{ get_medication_orders(bnf_code='0601') }}
),

-- GLP-1 RA classification by active ingredient, joined back on order id
glp1 AS (
    SELECT
        medication_order_id,
        glp1_drug,
        is_dual_gip_glp1
    FROM {{ ref('int_glp1_medications_all') }}
)

SELECT
    bo.person_id,
    bo.medication_order_id,
    bo.medication_statement_id,
    bo.order_date,
    bo.order_medication_name,
    bo.order_dose,
    bo.order_quantity_value,
    bo.order_quantity_unit,
    bo.order_duration_days,
    bo.statement_medication_name,
    bo.mapped_concept_code,
    bo.mapped_concept_display,
    bo.bnf_code,
    bo.bnf_name,

    -- Diabetes medication type classification (corrected BNF codes)
    CASE
        WHEN bo.bnf_code LIKE '0601011%' OR bo.bnf_code LIKE '0601012%' THEN 'INSULIN'  -- BNF 6.1.1: Insulins
        WHEN bo.bnf_code LIKE '0601021%' OR bo.bnf_code LIKE '0601022%' OR bo.bnf_code LIKE '0601023%' THEN 'ANTIDIABETIC'  -- BNF 6.1.2: Antidiabetic drugs
        WHEN bo.bnf_code LIKE '0601040%' THEN 'HYPOGLYCAEMIA_TREATMENT'   -- BNF 6.1.4: Treatment of hypoglycaemia
        WHEN bo.bnf_code LIKE '0601060%' THEN 'MONITORING'                -- BNF 6.1.6: Diabetic diagnostic and monitoring agents
        ELSE 'OTHER_DIABETES'
    END AS diabetes_medication_type,

    -- Antidiabetic drug class classification (BNF 6.1.2 subcodes).
    -- GLP-1 RAs are flagged from the SNOMED cluster (they sit within 6.1.2.3 by BNF).
    CASE
        WHEN g.medication_order_id IS NOT NULL THEN 'GLP1_RECEPTOR_AGONIST'
        WHEN bo.bnf_code LIKE '0601021%' THEN 'SULPHONYLUREAS'            -- 6.1.2.1: Sulphonylureas
        WHEN bo.bnf_code LIKE '0601022%' THEN 'BIGUANIDES'                -- 6.1.2.2: Biguanides (metformin)
        WHEN bo.bnf_code LIKE '0601023%' THEN 'OTHER_ANTIDIABETICS'       -- 6.1.2.3: Other antidiabetic drugs
        ELSE NULL
    END AS antidiabetic_drug_class,

    -- GLP-1 RA active ingredient (null for non-GLP-1 orders)
    g.glp1_drug,

    -- Insulin type classification (corrected BNF 6.1.1 subcodes)
    CASE
        WHEN bo.bnf_code LIKE '0601011%' THEN 'SHORT_ACTING'              -- 6.1.1.1: Short-acting insulins
        WHEN bo.bnf_code LIKE '0601012%' THEN 'INTERMEDIATE_LONG_ACTING'   -- 6.1.1.2: Intermediate and long-acting insulins
        ELSE NULL
    END AS insulin_type,

    -- Key medication flags (corrected BNF codes)
    CASE WHEN bo.bnf_code LIKE '0601011%' OR bo.bnf_code LIKE '0601012%' THEN TRUE ELSE FALSE END AS is_insulin,
    CASE WHEN bo.bnf_code LIKE '0601022%' THEN TRUE ELSE FALSE END AS is_metformin,
    CASE WHEN bo.bnf_code LIKE '0601021%' THEN TRUE ELSE FALSE END AS is_sulphonylurea,
    CASE WHEN g.medication_order_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_glp1_ra,
    COALESCE(g.is_dual_gip_glp1, FALSE) AS is_dual_gip_glp1,


    -- Order recency flags
    CASE
        WHEN DATEDIFF(day, bo.order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, bo.order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m

FROM base_orders bo
LEFT JOIN glp1 g
    ON bo.medication_order_id = g.medication_order_id
ORDER BY bo.person_id, bo.order_date DESC
