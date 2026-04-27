{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All lithium medication orders for bipolar disorder and severe depression.
Uses BNF 040203 and LIT_COD cluster (QOF lithium prescription codes).
*/

WITH bnf_orders AS (
    SELECT
        person_id, medication_order_id, medication_statement_id,
        order_date, order_medication_name, order_dose,
        order_quantity_value, order_quantity_unit, order_duration_days,
        statement_medication_name, mapped_concept_code, mapped_concept_display,
        bnf_code, bnf_name
    FROM ({{ get_medication_orders(bnf_code='040203') }}) bo
),

cluster_orders AS (
    SELECT
        person_id, medication_order_id, medication_statement_id,
        order_date, order_medication_name, order_dose,
        order_quantity_value, order_quantity_unit, order_duration_days,
        statement_medication_name, mapped_concept_code, mapped_concept_display,
        bnf_code, bnf_name
    FROM ({{ get_medication_orders(cluster_id='LIT_COD') }}) co
),

combined AS (
    SELECT * FROM bnf_orders
    UNION ALL
    SELECT * FROM cluster_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY medication_order_id ORDER BY bnf_code NULLS LAST) = 1
)

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,
    order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
    order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
    order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m
FROM combined
ORDER BY person_id, order_date DESC
