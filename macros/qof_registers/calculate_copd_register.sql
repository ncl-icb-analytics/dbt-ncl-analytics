{% macro calculate_copd_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates COPD register status at a given reference date.

    Implements full QOF COPD Rules 1-4:
    - Rule 1: EUNRESCOPD_DAT < 01/04/2023 → automatic inclusion
    - Rule 2: EUNRESCOPD_DAT >= 01/04/2023 + spirometry <0.7 within -93 to +186 days
    - Rule 3: Additional spirometry pathway (-186 to +365 days)
    - Rule 4: Based on EUNRESCOPD_DAT threshold

    EUNRESCOPD_DAT (Field 22):
    - If no resolved codes → COPD_DAT (earliest diagnosis)
    - Else → COPD1_DAT (earliest diagnosis after latest resolved)

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH copd_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code,
            is_resolved_code
        FROM {{ ref('int_copd_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    copd_person_aggregates AS (
        SELECT
            person_id,
            MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS copd_dat,
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS copdlat_dat,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date
        FROM copd_diagnoses_filtered
        GROUP BY person_id
    ),

    copd_qof_fields AS (
        SELECT
            person_id,
            copd_dat,
            copdlat_dat,
            latest_resolved_date,
            -- COPDRES_DAT: latest resolved after latest diagnosis
            CASE
                WHEN latest_resolved_date > copdlat_dat THEN latest_resolved_date
            END AS copdres_dat,
            -- COPDRES1_DAT: latest resolved after earliest diagnosis
            CASE
                WHEN latest_resolved_date > copd_dat THEN latest_resolved_date
            END AS copdres1_dat
        FROM copd_person_aggregates
    ),

    copd1_dat_calc AS (
        SELECT
            qf.person_id,
            MIN(df.clinical_effective_date) AS copd1_dat
        FROM copd_qof_fields qf
        INNER JOIN copd_diagnoses_filtered df
            ON qf.person_id = df.person_id
            AND df.is_diagnosis_code = TRUE
            AND qf.copdres1_dat < df.clinical_effective_date
        WHERE qf.copdres1_dat IS NOT NULL
        GROUP BY qf.person_id
    ),

    eunrescopd_dat_calc AS (
        SELECT
            qf.person_id,
            qf.copd_dat,
            qf.copdlat_dat,
            qf.copdres_dat,
            qf.copdres1_dat,
            c1.copd1_dat,
            -- EUNRESCOPD_DAT (Field 22): per exact QOF specification
            CASE
                WHEN qf.copdres_dat IS NULL AND qf.copdres1_dat IS NULL THEN qf.copd_dat
                ELSE COALESCE(c1.copd1_dat, qf.copd_dat)
            END AS eunrescopd_dat
        FROM copd_qof_fields qf
        LEFT JOIN copd1_dat_calc c1 ON qf.person_id = c1.person_id
    ),

    spirometry_filtered AS (
        SELECT
            person_id,
            clinical_effective_date AS spirometry_date,
            fev1_fvc_ratio,
            is_below_0_7,
            is_valid_spirometry
        FROM {{ ref('int_spirometry_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
          AND is_valid_spirometry = TRUE
          AND is_below_0_7 = TRUE
    ),

    -- Rule 1: Pre-April 2023 automatic inclusion
    rule_1_qualifiers AS (
        SELECT
            person_id,
            eunrescopd_dat,
            1 AS rule_number
        FROM eunrescopd_dat_calc
        WHERE eunrescopd_dat IS NOT NULL
          AND eunrescopd_dat < '2023-04-01'
    ),

    -- Patients for Rules 2-3 (post-April 2023)
    post_april_patients AS (
        SELECT *
        FROM eunrescopd_dat_calc
        WHERE eunrescopd_dat IS NOT NULL
          AND eunrescopd_dat >= '2023-04-01'
    ),

    -- Rule 2: Spirometry within -93 to +186 days
    rule_2_qualifiers AS (
        SELECT
            pap.person_id,
            pap.eunrescopd_dat,
            2 AS rule_number
        FROM post_april_patients pap
        INNER JOIN spirometry_filtered sf
            ON pap.person_id = sf.person_id
            AND sf.spirometry_date >= DATEADD('day', -93, pap.eunrescopd_dat)
            AND sf.spirometry_date <= DATEADD('day', 186, pap.eunrescopd_dat)
    ),

    -- Rule 3: Extended spirometry pathway (-186 to +365 days)
    rule_3_qualifiers AS (
        SELECT
            pap.person_id,
            pap.eunrescopd_dat,
            3 AS rule_number
        FROM post_april_patients pap
        INNER JOIN spirometry_filtered sf
            ON pap.person_id = sf.person_id
            AND sf.spirometry_date >= DATEADD('day', -186, pap.eunrescopd_dat)
            AND sf.spirometry_date <= DATEADD('day', 365, pap.eunrescopd_dat)
        WHERE pap.person_id NOT IN (SELECT person_id FROM rule_2_qualifiers)
    ),

    all_qualifiers AS (
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_1_qualifiers
        UNION ALL
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_2_qualifiers
        UNION ALL
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_3_qualifiers
    ),

    copd_register_logic AS (
        SELECT
            aq.person_id,
            'COPD' AS register_name,
            TRUE AS is_on_register
        FROM (
            SELECT person_id FROM all_qualifiers GROUP BY person_id
        ) aq
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM copd_register_logic

{% endmacro %}
