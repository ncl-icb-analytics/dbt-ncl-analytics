{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Serious Mental Illness (SMI) Register - QOF Mental Health Quality Measures**

Business Logic (QOF v50 MH001, aligned with the calculate_smi_register macro at reference = today):
- MH1_REG: has an MH_COD diagnosis (resolved or not — remission does not exclude from the register)
- MH2_REG: LIT_COD lithium in the last 6 months, not subsequently stopped (LITSTP_COD)
- No age restrictions for SMI register
- Based on legacy fct_person_dx_smi.sql
- Amended 8.4.26 KH Include 'Has_resolved_code' flag for use in SMI health checks

QOF Context:
Used for serious mental illness quality measures including:
- Mental health care monitoring and review
- Lithium therapy monitoring and safety
- Specialist mental health service coordination
- Recovery and rehabilitation planning

*/

WITH smi_diagnoses AS (
    -- Per QOF MH001: MH1_REG is "ever diagnosed" — no remission exclusion. HAS_MH_DIAGNOSIS = TRUE
    SELECT
        person_id,
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        -- MAX(clinical_effective_date) AS latest_diagnosis_date,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,
        -- MAX(clinical_effective_date) AS latest_diagnosis_date,
        --NULL::TIMESTAMP_NTZ AS latest_resolved_date,
        MAX(
            CASE WHEN is_resolved_code THEN clinical_effective_date END
        ) AS latest_resolved_date,
        --TRUE AS has_active_smi_diagnosis, See below for revised logic to account for resolved codes
         -- For SMI health checks flag whether the resolved code is reported on or after the latest diagnosis date
        COALESCE(
    (
        /* Case 1: both diagnosis and resolved exist, resolved is same or later */
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NOT NULL
        AND
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
        AND
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            >=
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
    )
    OR
    (
        /* Case 2: resolved exists and NO diagnosis exists */
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NOT NULL
        AND
        MAX(CASE WHEN is_diagnosis_code THEN 1 ELSE 0 END) = 0
    ),
    FALSE
) AS has_recent_resolved_code,
-- has_active_smi_diagnosis is simply the inverse of has_recent_resolved_code
      NOT COALESCE(
         (
              MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NOT NULL
              AND MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
              AND MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
                  >= MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
          )
          OR
          (
              MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NOT NULL
              AND MAX(CASE WHEN is_diagnosis_code THEN 1 ELSE 0 END) = 0
          ),
          FALSE
      ) AS has_active_smi_diagnosis,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END) AS all_smi_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END) AS all_smi_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_resolved_code THEN concept_code END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_resolved_code THEN concept_display END) AS all_resolved_concept_displays
    FROM {{ ref('int_smi_diagnoses_all') }}
    WHERE clinical_effective_date <= CURRENT_DATE()
        AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= CURRENT_DATE())
    GROUP BY person_id
),

-- MH2_REG: latest LITSTP_COD stop code, used to exclude stopped lithium
lithium_stops AS (
    SELECT
        person_id,
        MAX(CAST(clinical_effective_date AS DATE)) AS litstp_dat
    FROM ({{ get_observations("'LITSTP_COD'", source='PCD') }})
    WHERE clinical_effective_date <= CURRENT_DATE()
        AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= CURRENT_DATE())
    GROUP BY person_id
),

-- Lithium (LIT_COD) orders in the 6 months up to today (LIT_DAT > today - 6 months)
lithium_recent AS (
    SELECT
        lith.person_id,
        MAX(CAST(lith.order_date AS DATE)) AS latest_lithium_order_date,
        MIN(CAST(lith.order_date AS DATE)) AS earliest_recent_lithium_date,
        COUNT(*) AS recent_lithium_orders_count,
        ARRAY_AGG(DISTINCT lith.mapped_concept_code)
            AS all_lithium_concept_codes,
        ARRAY_AGG(DISTINCT lith.mapped_concept_display)
            AS all_lithium_concept_displays
    FROM {{ ref('int_lithium_medications_all') }} AS lith
    WHERE CAST(lith.order_date AS DATE) <= CURRENT_DATE()
        AND CAST(lith.order_date AS DATE) > DATEADD('month', -6, CURRENT_DATE())
        AND (lith.date_recorded IS NULL OR CAST(lith.date_recorded AS DATE) <= CURRENT_DATE())
    GROUP BY lith.person_id
),

-- Exclude patients whose lithium was stopped after their latest order (LITSTP_DAT > LIT_DAT)
lithium_medications AS (
    SELECT lr.*
    FROM lithium_recent AS lr
    LEFT JOIN lithium_stops AS s
        ON lr.person_id = s.person_id
        AND s.litstp_dat > lr.latest_lithium_order_date
    WHERE s.person_id IS NULL
),

combined_smi_eligibility AS (

    SELECT
        age.age,
        smi.earliest_diagnosis_date,

        -- Mental health diagnosis details
        smi.latest_diagnosis_date,
        smi.latest_resolved_date,
        smi.has_active_smi_diagnosis,
        smi.has_recent_resolved_code,
        lith.latest_lithium_order_date,

        -- Lithium therapy details
        lith.recent_lithium_orders_count,
        lith.all_lithium_concept_codes,
        lith.all_lithium_concept_displays,
        
       -- SMI diagnosis details
        smi.all_smi_concept_codes,
        smi.all_smi_concept_displays,
        smi.all_resolved_concept_codes,
        smi.all_resolved_concept_displays,
        
        -- SMI Register Logic: Any SMI diagnosis (active or not) OR recent lithium therapy
       
        COALESCE(smi.person_id, lith.person_id) AS person_id,
        (
            -- MH1_REG: has an MH_COD diagnosis (resolved or not). MH2_REG: recent lithium, not stopped.
            smi.latest_diagnosis_date IS NOT NULL
            OR
            (lith.recent_lithium_orders_count > 0)
        ) AS is_on_register,
        smi.latest_diagnosis_date IS NOT NULL OR smi.latest_resolved_date IS NOT NULL AS has_mh_diagnosis,
        lith.recent_lithium_orders_count > 0 AS is_on_lithium

    FROM smi_diagnoses AS smi
    FULL OUTER JOIN lithium_medications AS lith
        ON smi.person_id = lith.person_id
    INNER JOIN {{ ref('dim_person') }} AS p
        ON COALESCE(smi.person_id, lith.person_id) = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON COALESCE(smi.person_id, lith.person_id) = age.person_id
)

-- Final selection: Only include patients on the SMI register
SELECT
    cse.person_id,
    cse.age,
    cse.is_on_register,
    cse.is_on_lithium,
    cse.has_mh_diagnosis,
    cse.has_recent_resolved_code,
    cse.has_active_smi_diagnosis,
    cse.earliest_diagnosis_date,
    cse.latest_diagnosis_date,
    cse.latest_resolved_date,
    cse.latest_lithium_order_date,
    cse.all_smi_concept_codes AS all_mh_concept_codes,
    cse.all_smi_concept_displays AS all_mh_concept_displays,
    cse.all_resolved_concept_codes,
    cse.all_resolved_concept_displays,
    cse.all_lithium_concept_codes,
    cse.all_lithium_concept_displays

FROM combined_smi_eligibility AS cse
WHERE cse.is_on_register = TRUE
