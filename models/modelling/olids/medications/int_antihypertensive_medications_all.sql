{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All antihypertensive medication orders for hypertension management.
Uses ANTIHYPERTENSIVE_MEDICATIONS cluster from combined codesets.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

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
    cluster_id,
    bnf_code,
    bnf_name

FROM (
    {{ get_medication_orders(cluster_id='ANTIHYPERTENSIVE_MEDICATIONS') }}
) base_orders
ORDER BY person_id, order_date DESC
