{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'pds', 'olids']
    )
}}

/*
PDS/OLIDS Practice Registration Comparison

Compares patient registration counts between Personal Demographics Service and OLIDS
to identify discrepancies at practice level.

Discrepancies may indicate:
- Data quality issues in either system
- Timing differences in data updates
- Practices with incomplete OLIDS coverage
- Registration processing delays

Highlights practices with >=20% discrepancy for investigation.
*/

WITH pds_counts AS (
    -- Count active patients per practice from Personal Demographics Service
    SELECT
        org.organisation_code as practice_code,
        org.organisation_name as practice_name,
        COUNT(DISTINCT person.pseudo_nhs_number) as pds_patient_count
    FROM {{ ref('stg_pds_pds_person') }} person
    INNER JOIN {{ ref('stg_pds_pds_patient_care_practice') }} practice
        ON person.pseudo_nhs_number = practice.pseudo_nhs_number
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} org
        ON practice.primary_care_provider = org.organisation_code
    WHERE (
        practice.primary_care_provider_business_effective_to_date IS NULL
        OR practice.primary_care_provider_business_effective_to_date >= CURRENT_DATE()
    )
    GROUP BY
        org.organisation_code,
        org.organisation_name
),

olids_current_registrations AS (
    -- Get the current registration for each patient from OLIDS
    SELECT
        p.sk_patient_id,
        o.organisation_code as practice_code,
        o.name as practice_name
    FROM {{ ref('stg_olids_patient_registered_practitioner_in_role') }} prpr
    INNER JOIN {{ ref('stg_olids_patient') }} p
        ON prpr.patient_id = p.id
    INNER JOIN {{ ref('stg_olids_organisation') }} o
        ON prpr.organisation_id = o.id
    WHERE
        prpr.start_date IS NOT NULL
        AND p.sk_patient_id IS NOT NULL
        AND (
            prpr.end_date IS NULL
            OR prpr.end_date > CURRENT_DATE()
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.sk_patient_id
        ORDER BY
            prpr.start_date DESC,
            prpr.id DESC
    ) = 1
),

olids_counts AS (
    -- Count distinct patients per practice from OLIDS
    SELECT
        practice_code,
        practice_name,
        COUNT(DISTINCT sk_patient_id) as olids_patient_count
    FROM olids_current_registrations
    GROUP BY
        practice_code,
        practice_name
)

-- Calculate discrepancies for all practices
SELECT
    COALESCE(pds.practice_code, olids.practice_code) as practice_code,
    COALESCE(pds.practice_name, olids.practice_name) as practice_name,
    COALESCE(pds.pds_patient_count, 0) as pds_patient_count,
    COALESCE(olids.olids_patient_count, 0) as olids_patient_count,
    pds.pds_patient_count - COALESCE(olids.olids_patient_count, 0) as difference,
    CASE
        WHEN olids.olids_patient_count = 0 OR olids.olids_patient_count IS NULL THEN NULL
        ELSE ROUND(
            (pds.pds_patient_count - olids.olids_patient_count) * 100.0 / olids.olids_patient_count,
            2
        )
    END as percent_difference,
    CASE
        WHEN ABS(COALESCE(
            (pds.pds_patient_count - olids.olids_patient_count) * 100.0 / NULLIF(olids.olids_patient_count, 0),
            0
        )) >= 20 THEN TRUE
        ELSE FALSE
    END as has_significant_discrepancy
FROM pds_counts pds
FULL OUTER JOIN olids_counts olids
    ON pds.practice_code = olids.practice_code
