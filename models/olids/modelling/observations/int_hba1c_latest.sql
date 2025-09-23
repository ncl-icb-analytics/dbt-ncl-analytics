{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid HbA1c measurement per person.
Uses the comprehensive int_hba1c_all model and filters to most recent valid HbA1c.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    hba1c_value,
    hba1c_value_ifcc_standardised,
    hba1c_dcct_value,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    result_unit_display,
    hba1c_result_display,
    hba1c_category,
    indicates_diabetes,
    meets_qof_target,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_hba1c_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_hba1c

WHERE is_valid_hba1c = TRUE
