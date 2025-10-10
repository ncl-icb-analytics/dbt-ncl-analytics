{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

/*
Current repeat medications within polypharmacy scope (BNF chapters 1-4, 6-10).

Filters current repeat medications to only those in polypharmacy scope using
NHSBSA standard BNF chapter inclusion list. Significant row reduction occurs
at this layer through the BNF scope INNER JOIN.

Grain: One row per person × current polypharmacy-scope medication
*/

SELECT
    mo.person_id,
    mo.mapped_concept_code,
    bnf.bnf_code,
    bnf.bnf_chapter,
    bnf.bnf_name,
    mo.latest_order_date,
    mo.latest_duration
FROM {{ ref('int_medication_orders_repeat_current') }} mo
INNER JOIN {{ ref('int_polypharmacy_medications_list') }} bnf
    ON mo.mapped_concept_code = bnf.snomed_code
