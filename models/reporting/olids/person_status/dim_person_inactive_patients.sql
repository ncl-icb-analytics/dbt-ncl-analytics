{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'inactive'],
        cluster_by=['person_id'])
}}

/*
Person Inactive Patients Dimension Table

Thin wrapper over dim_person_demographics — filters to inactive patients only.
Replaces previous implementation that re-joined dim_person, stg_olids_patient,
and dim_person_historical_practice with expensive ROW_NUMBER() and array indexing.

Includes only deceased patients and patients from closed/obsolete practices.
Excludes dummy patients as they are not considered real inactive patients.

Ref: https://github.com/ncl-icb-analytics/dbt-ncl-analytics/issues/641
*/

SELECT
    -- Core Identifiers
    person_id,
    sk_patient_id,

    -- Status
    is_active,
    is_deceased,
    is_dummy_patient,
    inactive_reason,

    -- Demographics
    birth_year,
    death_year,
    death_date_approx,
    gender,

    -- Practice Registration
    practice_code,
    practice_name,
    practice_postcode,
    registration_start_date,
    registration_end_date,

    -- PCN / ICB
    pcn_code,
    pcn_name,
    icb_code,
    icb_name,

    -- Geography
    borough_registered

FROM {{ ref('dim_person_demographics') }}
WHERE is_active = FALSE
    AND is_dummy_patient = FALSE
    AND inactive_reason IS NOT NULL
