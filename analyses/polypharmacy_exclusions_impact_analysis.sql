/*
Polypharmacy Exclusions Impact Analysis
Compares OLD (MODELLING/REPORTING) vs NEW (DEV__MODELLING/DEV__REPORTING) polypharmacy counts

Purpose: Show impact of granular BNF chapter exclusions (GitHub issue #158)

OLD state: Simple chapter inclusion (chapters 1-4, 6-10)
NEW state: Granular exclusion-based approach (excludes vitamins, contraceptives, etc.)

Run this BEFORE merging to production to share impact with colleagues.
*/

-- ============================================================================
-- 1. SNOMED Code Scope Comparison
-- ============================================================================
WITH old_scope AS (
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        COUNT(DISTINCT snomed_code) AS snomed_code_count,
        COUNT(DISTINCT bnf_chapter) AS chapter_count,
        LISTAGG(DISTINCT bnf_chapter, ', ') WITHIN GROUP (ORDER BY bnf_chapter) AS chapters_included
    FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_list
),

new_scope AS (
    SELECT
        'NEW (exclusion-based)' AS version,
        COUNT(DISTINCT snomed_code) AS snomed_code_count,
        COUNT(DISTINCT bnf_chapter) AS chapter_count,
        LISTAGG(DISTINCT bnf_chapter, ', ') WITHIN GROUP (ORDER BY bnf_chapter) AS chapters_included
    FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_list
),

scope_comparison AS (
    SELECT * FROM old_scope
    UNION ALL
    SELECT * FROM new_scope
)

SELECT
    '=== SCOPE: int_polypharmacy_medications_list (SNOMED Codes) ===' AS section,
    version,
    snomed_code_count,
    chapter_count,
    chapters_included,
    snomed_code_count - LAG(snomed_code_count) OVER (ORDER BY version DESC) AS snomed_code_change,
    ROUND(100.0 * (snomed_code_count - LAG(snomed_code_count) OVER (ORDER BY version DESC)) /
          NULLIF(LAG(snomed_code_count) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM scope_comparison

UNION ALL

-- ============================================================================
-- 2. Current Medications by Person (with person-level changes)
-- ============================================================================
SELECT
    '=== CURRENT MEDS: int_polypharmacy_medications_current (Persons) ===' AS section,
    version,
    person_count AS snomed_code_count,
    NULL AS chapter_count,
    CONCAT('Lost: ', lost_persons, ' | Gained: ', gained_persons, ' | Net: ', net_change) AS chapters_included,
    net_change AS snomed_code_change,
    ROUND(100.0 * net_change / NULLIF(LAG(person_count) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    WITH old_persons AS (
        SELECT DISTINCT person_id
        FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_current
    ),
    new_persons AS (
        SELECT DISTINCT person_id
        FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_current
    ),
    person_changes AS (
        SELECT
            (SELECT COUNT(*) FROM old_persons WHERE person_id NOT IN (SELECT person_id FROM new_persons)) AS lost_persons,
            (SELECT COUNT(*) FROM new_persons WHERE person_id NOT IN (SELECT person_id FROM old_persons)) AS gained_persons
    )
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        (SELECT COUNT(*) FROM old_persons) AS person_count,
        0 AS lost_persons,
        0 AS gained_persons,
        0 AS net_change

    UNION ALL

    SELECT
        'NEW (exclusion-based)' AS version,
        (SELECT COUNT(*) FROM new_persons) AS person_count,
        pc.lost_persons,
        pc.gained_persons,
        pc.gained_persons - pc.lost_persons AS net_change
    FROM person_changes pc
)

UNION ALL

-- ============================================================================
-- 3. Total Medication Orders (Person × Medication combinations)
-- ============================================================================
SELECT
    '=== CURRENT MEDS: int_polypharmacy_medications_current (Total Rows) ===' AS section,
    version,
    row_count AS snomed_code_count,
    NULL AS chapter_count,
    NULL AS chapters_included,
    row_count - LAG(row_count) OVER (ORDER BY version DESC) AS snomed_code_change,
    ROUND(100.0 * (row_count - LAG(row_count) OVER (ORDER BY version DESC)) /
          NULLIF(LAG(row_count) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        COUNT(*) AS row_count
    FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_current

    UNION ALL

    SELECT
        'NEW (exclusion-based)' AS version,
        COUNT(*) AS row_count
    FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_medications_current
)

UNION ALL

-- ============================================================================
-- 4. Polypharmacy Status: Current Counts (with person-level changes)
-- ============================================================================
SELECT
    '=== POLYPHARMACY: int_polypharmacy_current (All Persons) ===' AS section,
    version,
    total_persons AS snomed_code_count,
    NULL AS chapter_count,
    CONCAT(status_breakdown, ' | Lost: ', lost_persons, ' | Gained: ', gained_persons) AS chapters_included,
    net_change AS snomed_code_change,
    ROUND(100.0 * net_change / NULLIF(LAG(total_persons) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    WITH old_persons AS (
        SELECT DISTINCT person_id
        FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
    ),
    new_persons AS (
        SELECT DISTINCT person_id
        FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
    ),
    person_changes AS (
        SELECT
            (SELECT COUNT(*) FROM old_persons WHERE person_id NOT IN (SELECT person_id FROM new_persons)) AS lost_persons,
            (SELECT COUNT(*) FROM new_persons WHERE person_id NOT IN (SELECT person_id FROM old_persons)) AS gained_persons
    ),
    old_summary AS (
        SELECT
            'OLD (chapters 1-4, 6-10)' AS version,
            COUNT(*) AS total_persons,
            SUM(CASE WHEN is_polypharmacy_5plus THEN 1 ELSE 0 END) AS count_5plus,
            SUM(CASE WHEN is_polypharmacy_10plus THEN 1 ELSE 0 END) AS count_10plus
        FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
    ),
    new_summary AS (
        SELECT
            'NEW (exclusion-based)' AS version,
            COUNT(*) AS total_persons,
            SUM(CASE WHEN is_polypharmacy_5plus THEN 1 ELSE 0 END) AS count_5plus,
            SUM(CASE WHEN is_polypharmacy_10plus THEN 1 ELSE 0 END) AS count_10plus
        FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
    )
    SELECT
        version,
        total_persons,
        CONCAT('5+: ', count_5plus, ' | 10+: ', count_10plus) AS status_breakdown,
        0 AS lost_persons,
        0 AS gained_persons,
        0 AS net_change
    FROM old_summary

    UNION ALL

    SELECT
        ns.version,
        ns.total_persons,
        CONCAT('5+: ', ns.count_5plus, ' | 10+: ', ns.count_10plus) AS status_breakdown,
        pc.lost_persons,
        pc.gained_persons,
        pc.gained_persons - pc.lost_persons AS net_change
    FROM new_summary ns
    CROSS JOIN person_changes pc
)

UNION ALL

-- ============================================================================
-- 4a. Polypharmacy 5+ Threshold: Person-Level Changes
-- ============================================================================
SELECT
    '=== POLYPHARMACY: int_polypharmacy_current (5+ medications) ===' AS section,
    version,
    count_5plus AS snomed_code_count,
    NULL AS chapter_count,
    CONCAT('Lost: ', lost_5plus, ' | Gained: ', gained_5plus, ' | Net: ', net_change_5plus) AS chapters_included,
    net_change_5plus AS snomed_code_change,
    ROUND(100.0 * net_change_5plus / NULLIF(LAG(count_5plus) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    WITH old_5plus AS (
        SELECT DISTINCT person_id
        FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
        WHERE is_polypharmacy_5plus = TRUE
    ),
    new_5plus AS (
        SELECT DISTINCT person_id
        FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
        WHERE is_polypharmacy_5plus = TRUE
    ),
    changes_5plus AS (
        SELECT
            (SELECT COUNT(*) FROM old_5plus WHERE person_id NOT IN (SELECT person_id FROM new_5plus)) AS lost_5plus,
            (SELECT COUNT(*) FROM new_5plus WHERE person_id NOT IN (SELECT person_id FROM old_5plus)) AS gained_5plus
    )
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        (SELECT COUNT(*) FROM old_5plus) AS count_5plus,
        0 AS lost_5plus,
        0 AS gained_5plus,
        0 AS net_change_5plus

    UNION ALL

    SELECT
        'NEW (exclusion-based)' AS version,
        (SELECT COUNT(*) FROM new_5plus) AS count_5plus,
        c.lost_5plus,
        c.gained_5plus,
        c.gained_5plus - c.lost_5plus AS net_change_5plus
    FROM changes_5plus c
)

UNION ALL

-- ============================================================================
-- 4b. Complex Polypharmacy 10+ Threshold: Person-Level Changes
-- ============================================================================
SELECT
    '=== POLYPHARMACY: int_polypharmacy_current (10+ medications) ===' AS section,
    version,
    count_10plus AS snomed_code_count,
    NULL AS chapter_count,
    CONCAT('Lost: ', lost_10plus, ' | Gained: ', gained_10plus, ' | Net: ', net_change_10plus) AS chapters_included,
    net_change_10plus AS snomed_code_change,
    ROUND(100.0 * net_change_10plus / NULLIF(LAG(count_10plus) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    WITH old_10plus AS (
        SELECT DISTINCT person_id
        FROM MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
        WHERE is_polypharmacy_10plus = TRUE
    ),
    new_10plus AS (
        SELECT DISTINCT person_id
        FROM DEV__MODELLING.OLIDS_MEDICATIONS.int_polypharmacy_current
        WHERE is_polypharmacy_10plus = TRUE
    ),
    changes_10plus AS (
        SELECT
            (SELECT COUNT(*) FROM old_10plus WHERE person_id NOT IN (SELECT person_id FROM new_10plus)) AS lost_10plus,
            (SELECT COUNT(*) FROM new_10plus WHERE person_id NOT IN (SELECT person_id FROM old_10plus)) AS gained_10plus
    )
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        (SELECT COUNT(*) FROM old_10plus) AS count_10plus,
        0 AS lost_10plus,
        0 AS gained_10plus,
        0 AS net_change_10plus

    UNION ALL

    SELECT
        'NEW (exclusion-based)' AS version,
        (SELECT COUNT(*) FROM new_10plus) AS count_10plus,
        c.lost_10plus,
        c.gained_10plus,
        c.gained_10plus - c.lost_10plus AS net_change_10plus
    FROM changes_10plus c
)

UNION ALL

-- ============================================================================
-- 5. Published Reporting Layer
-- ============================================================================
SELECT
    '=== REPORTING: fct_person_polypharmacy_current (All Persons) ===' AS section,
    version,
    total_persons AS snomed_code_count,
    NULL AS chapter_count,
    status_breakdown AS chapters_included,
    total_persons - LAG(total_persons) OVER (ORDER BY version DESC) AS snomed_code_change,
    ROUND(100.0 * (total_persons - LAG(total_persons) OVER (ORDER BY version DESC)) /
          NULLIF(LAG(total_persons) OVER (ORDER BY version DESC), 0), 2) AS pct_change
FROM (
    SELECT
        'OLD (chapters 1-4, 6-10)' AS version,
        COUNT(*) AS total_persons,
        CONCAT(
            '5+: ', SUM(CASE WHEN is_polypharmacy_5plus THEN 1 ELSE 0 END),
            ' | 10+: ', SUM(CASE WHEN is_polypharmacy_10plus THEN 1 ELSE 0 END)
        ) AS status_breakdown
    FROM REPORTING.OLIDS_PERSON_STATUS.fct_person_polypharmacy_current

    UNION ALL

    SELECT
        'NEW (exclusion-based)' AS version,
        COUNT(*) AS total_persons,
        CONCAT(
            '5+: ', SUM(CASE WHEN is_polypharmacy_5plus THEN 1 ELSE 0 END),
            ' | 10+: ', SUM(CASE WHEN is_polypharmacy_10plus THEN 1 ELSE 0 END)
        ) AS status_breakdown
    FROM DEV__REPORTING.OLIDS_PERSON_STATUS.fct_person_polypharmacy_current
)

ORDER BY section, version DESC;
