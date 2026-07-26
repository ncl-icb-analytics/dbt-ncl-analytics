{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Cost Index Semantic View
    ========================

    Patient-month spend by service. SLAM actual provider cost starts in
    2021-04 and excludes the latest incomplete month. EPD prescribing starts
    in 2018-04 but is about 12 months behind SLAM, so recent combined totals
    are asymmetric. About 1.5% of cost is Unmapped service.
#}

TABLES(
    cost AS {{ ref('fct_person_cost_index_monthly') }}
        PRIMARY KEY (sk_patient_id, activity_month, service_grouping, service, is_patient_attributable, cost_basis, cost_source, activity_unit)
        COMMENT = 'Patient-level monthly cost and activity by service'
)

DIMENSIONS(
    cost.sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',
    cost.activity_month AS activity_month COMMENT = 'First day of the activity month',
    cost.service_grouping AS service_grouping COMMENT = 'Service grouping',
    cost.service AS service COMMENT = 'Service',
    cost.is_patient_attributable AS is_patient_attributable COMMENT = 'TRUE for patient-attributable cost. Filter TRUE for patient-level analysis.',
    cost.cost_basis AS cost_basis COMMENT = 'Cost basis: actual, proxy, nominal, or tariff',
    cost.cost_source AS cost_source COMMENT = 'Cost source: SLAM or EPD',
    cost.activity_unit AS activity_unit COMMENT = 'Unit used for total activity'
)

METRICS(
    cost.total_cost AS SUM(cost.total_cost) COMMENT = 'Total cost',
    cost.total_activity AS SUM(cost.total_activity) COMMENT = 'Total activity',
    cost.patient_count AS COUNT(DISTINCT cost.sk_patient_id) COMMENT = 'Distinct people'
)

COMMENT = 'Cost Index Semantic View - monthly patient-level SLAM and EPD spend. SLAM actual provider cost starts 2021-04 and excludes the latest incomplete month. EPD prescribing starts 2018-04 but is about 12 months behind, so recent combined totals are asymmetric.'
AI_SQL_GENERATION 'Cost is monthly patient-level spend. SLAM is actual provider cost from 2021-04; the latest incomplete month is excluded. EPD prescribing is available from 2018-04 but is about 12 months behind SLAM, so recent combined totals are asymmetric. Filter is_patient_attributable = TRUE for patient-level analysis. About 1.5% of cost is Unmapped service. CROSS-DATASET COHORT INTERSECTION: join a sem_olids_population CTE and this view CTE on sk_patient_id, aggregate the result, never return sk_patient_id, and apply HAVING COUNT(DISTINCT sk_patient_id) > 5.'
AI_QUESTION_CATEGORIZATION 'Use this view for: monthly spend, activity, service mix, SLAM actual provider cost, EPD prescribing cost, and costs of OLIDS-defined cohorts via sk_patient_id linkage.'
