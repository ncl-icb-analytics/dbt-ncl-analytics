{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'pds', 'olids']
    )
}}

/*
PDS/OLIDS Practice Registration Comparison

Compares patient registration counts between PDS and OLIDS to identify discrepancies.
Uses episode_of_care filtered to registration type episodes.
Aligns with PDS comparison methodology from validation scripts.

Discrepancies may indicate:
- Data quality issues in either system
- Timing differences in data updates
- Practices with incomplete OLIDS coverage
- Registration processing delays
*/

WITH pds_registrations AS (
    -- Get current registrations from PDS with proper merger and death filtering
    SELECT
        reg.primary_care_provider AS practice_code,
        prac.organisation_name AS practice_name,
        reg.sk_patient_id,
        -- Handle NHS number mergers to get canonical person
        COALESCE(merg.sk_patient_id_superseded, reg.sk_patient_id) AS merged_sk_patient_id
    FROM {{ ref('stg_pds_pds_patient_care_practice') }} reg
    LEFT JOIN {{ ref('stg_pds_pds_person_merger') }} merg
        ON reg.sk_patient_id = merg.sk_patient_id
    LEFT JOIN {{ ref('stg_pds_pds_person') }} per
        ON reg.sk_patient_id = per.sk_patient_id
        AND per.person_business_effective_from_date <= COALESCE(reg.primary_care_provider_business_effective_to_date, '9999-12-31')
        AND COALESCE(per.person_business_effective_to_date, '9999-12-31') >= reg.primary_care_provider_business_effective_from_date
        AND CURRENT_DATE() BETWEEN per.person_business_effective_from_date
            AND COALESCE(per.person_business_effective_to_date, '9999-12-31')
    LEFT JOIN {{ ref('stg_pds_pds_reason_for_removal') }} reas
        ON reg.sk_patient_id = reas.sk_patient_id
        AND reas.reason_for_removal_business_effective_from_date <= COALESCE(reg.primary_care_provider_business_effective_to_date, '9999-12-31')
        AND COALESCE(reas.reason_for_removal_business_effective_to_date, '9999-12-31') >= reg.primary_care_provider_business_effective_from_date
        AND CURRENT_DATE() BETWEEN reas.reason_for_removal_business_effective_from_date
            AND COALESCE(reas.reason_for_removal_business_effective_to_date, '9999-12-31')
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} prac
        ON reg.primary_care_provider = prac.organisation_code
    INNER JOIN {{ ref('stg_dictionary_dbo_organisation') }} icb
        ON prac.sk_parentorg_id = icb.sk_organisationid
        AND icb.organisation_code = '93C'
        AND prac.enddate IS NULL
    WHERE per.death_status IS NULL
        AND per.date_of_death IS NULL
        AND reg.sk_patient_id IS NOT NULL
        AND CURRENT_DATE() BETWEEN reg.primary_care_provider_business_effective_from_date
            AND COALESCE(reg.primary_care_provider_business_effective_to_date, '9999-12-31')
        AND reas.reason_for_removal IS NULL
),

pds_counts AS (
    SELECT
        practice_code,
        practice_name,
        COUNT(DISTINCT sk_patient_id) AS pds_unmerged_persons,
        COUNT(DISTINCT merged_sk_patient_id) AS pds_merged_persons
    FROM pds_registrations
    GROUP BY practice_code, practice_name
),

olids_counts AS (
    -- Count patients per practice from OLIDS using new registration logic
    -- Uses episode_of_care filtered to registration type with deceased handling
    SELECT
        reg.practice_ods_code AS practice_code,
        reg.practice_name,
        COUNT(DISTINCT reg.sk_patient_id) AS olids_registered_patients
    FROM {{ ref('int_patient_registrations') }} reg
    WHERE reg.is_current_registration = TRUE
        AND reg.practice_ods_code IS NOT NULL
    GROUP BY
        reg.practice_ods_code,
        reg.practice_name
)

-- Calculate discrepancies for all practices comparing merged PDS counts to OLIDS
SELECT
    COALESCE(pds.practice_code, olids.practice_code) AS practice_code,
    COALESCE(pds.practice_name, olids.practice_name) AS practice_name,
    COALESCE(pds.pds_unmerged_persons, 0) AS pds_unmerged_persons,
    COALESCE(pds.pds_merged_persons, 0) AS pds_merged_persons,
    COALESCE(olids.olids_registered_patients, 0) AS olids_patient_count,
    -- Use merged PDS count for comparison as it accounts for NHS number changes
    COALESCE(olids.olids_registered_patients, 0) - COALESCE(pds.pds_merged_persons, 0) AS difference,
    CASE
        WHEN pds.pds_merged_persons = 0 OR pds.pds_merged_persons IS NULL THEN NULL
        ELSE ROUND(
            (COALESCE(olids.olids_registered_patients, 0) - pds.pds_merged_persons) * 100.0 / pds.pds_merged_persons,
            2
        )
    END AS percent_difference,
    CASE
        WHEN ABS(COALESCE(
            (COALESCE(olids.olids_registered_patients, 0) - pds.pds_merged_persons) * 100.0 / NULLIF(pds.pds_merged_persons, 0),
            0
        )) >= 20 THEN TRUE
        ELSE FALSE
    END AS has_significant_discrepancy
FROM pds_counts pds
FULL OUTER JOIN olids_counts olids
    ON pds.practice_code = olids.practice_code
ORDER BY ABS(COALESCE(
    (COALESCE(olids.olids_registered_patients, 0) - pds.pds_merged_persons) * 100.0 / NULLIF(pds.pds_merged_persons, 0),
    0
)) DESC
