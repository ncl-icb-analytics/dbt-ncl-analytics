{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'opt_out', 'gdpr'],
        cluster_by=['person_id'])
}}

/*
Type 1 opt-out status dimension.
Holds latest Type 1 opt-out status (dissent from secondary use of primary care data).
Only includes persons with Type 1 opt-out records.
*/

WITH latest_opt_out_status AS (
    SELECT
        opt.person_id,
        pp.sk_patient_id,
        opt.clinical_effective_date,
        opt.concept_code,
        opt.code_description,
        opt.source_cluster_id,

        CASE
            WHEN opt.source_cluster_id IN ('OPT_OUT_TYPE_1_DISSENT', 'OPT_OUT_TYPE_1_DISSENT_WITHDRAWAL') THEN TRUE
            ELSE FALSE
        END AS is_opted_out,

        CASE
            WHEN opt.source_cluster_id IN ('OPT_OUT_TYPE_1_DISSENT', 'OPT_OUT_TYPE_1_DISSENT_WITHDRAWAL') THEN 'Opted Out'
            ELSE 'Not Opted Out'
        END AS opt_out_status

    FROM {{ ref('int_opt_out_type_1_all') }} opt
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON opt.person_id = pp.person_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY opt.person_id
        ORDER BY opt.clinical_effective_date DESC, opt.id DESC
    ) = 1
)

SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date AS latest_opt_out_date,
    concept_code,
    code_description,
    source_cluster_id,
    is_opted_out,
    opt_out_status
FROM latest_opt_out_status
