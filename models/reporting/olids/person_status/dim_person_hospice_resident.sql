{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'hospice', 'residence'],
        cluster_by=['person_id'])
}}

-- Person Hospice Resident Dimension Table
-- Latest "lives in hospice" status for persons with a HOSPICE_RESIDENT_COD record.
-- "Lives in hospice" (1024771000000108) is a member of the RESIDE_COD "type of residence"
-- cluster, so residence is mutually exclusive: a later RESIDE_COD code of any other type,
-- or a HOSPICE_DISCHARGE_COD, supersedes hospice residency.
-- Note: RESIDE_COD mixes places with housing-status findings (e.g. 'Housing adequate'),
-- so a later finding-type code can supersede residency (same limitation as dim_person_homeless).

WITH hospice_resident AS (
    -- Latest hospice resident event per person
    SELECT
        person_id,
        clinical_effective_date,
        concept_code,
        code_description,
        source_cluster_id
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id = 'HOSPICE_RESIDENT_COD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

other_residence AS (
    -- Latest non-hospice residence code; supersedes hospice residency when more recent
    SELECT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_other_residence_date
    FROM ({{ get_observations("'RESIDE_COD'", source='UKHSA_COVID') }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.mapped_concept_code != '1024771000000108'
    GROUP BY obs.person_id
),

discharge AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_discharge_date
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id = 'HOSPICE_DISCHARGE_COD'
    GROUP BY person_id
),

resident_status AS (
    SELECT
        hr.person_id,
        hr.clinical_effective_date AS latest_hospice_resident_date,
        o.latest_other_residence_date,
        d.latest_discharge_date,
        hr.concept_code,
        hr.code_description,
        hr.source_cluster_id,
        -- Current resident only if the hospice event is the most recent residence signal
        hr.clinical_effective_date >= COALESCE(o.latest_other_residence_date, '1900-01-01')
            AND hr.clinical_effective_date >= COALESCE(d.latest_discharge_date, '1900-01-01') AS is_hospice_resident
    FROM hospice_resident hr
    LEFT JOIN other_residence o
        ON hr.person_id = o.person_id
    LEFT JOIN discharge d
        ON hr.person_id = d.person_id
)

SELECT
    rs.person_id,
    pp.sk_patient_id,
    rs.latest_hospice_resident_date,
    rs.latest_other_residence_date,
    rs.latest_discharge_date,
    rs.concept_code,
    rs.code_description,
    rs.source_cluster_id,
    rs.is_hospice_resident,
    CASE
        WHEN rs.is_hospice_resident THEN 'Hospice Resident'
        ELSE 'No Longer Hospice Resident'
    END AS hospice_resident_status
FROM resident_status rs
JOIN {{ ref('int_patient_person_unique') }} pp
    ON rs.person_id = pp.person_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rs.person_id
    ORDER BY pp.sk_patient_id
) = 1
