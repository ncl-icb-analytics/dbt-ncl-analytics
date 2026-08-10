{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Children with complexity criteria for population segmentation.
-- Grain: one row per person aged under 18 in dim_person_demographics,
-- regardless of registration status (filter is_active = TRUE for the
-- currently registered population). All under-18s are kept, with each
-- criterion exposed as a count and flag, so the marginal contribution of
-- any single criterion can be measured; meets_any_criterion drives the
-- children with complexity segment.
--
-- Criteria (any qualifies):
--   2+ qualifying LTCs (int_segmentation_ltc_count; 2+ rather than the
--       circulated 1+ so a single LTC reads as health needs (segment 2)
--       rather than complexity, keeping segment 2 reachable)
--   1+ coded complexity diagnosis (children_complexity_diagnosis_codes seed,
--       ever recorded; the NWL CLDCHN code list, per the spec)
--   5+ attended paediatric outpatient appointments in 12 months
--       (paediatric_treatment_function_codes seed)
--   attended outpatient care across 2+ treatment-function specialties
--       in 12 months (any specialty, fct_person_sus_op_recent)
--   1+ mental health inpatient stay in 12 months (MHSDS spells counted by
--       start date; lifetime count also exposed)
--   7+ attended community service contacts in 12 months (CSDS)
--
-- 12-month windows end on the latest activity date in each source to
-- account for reporting lag. SUS/MHSDS/CSDS activity joins on
-- sk_patient_id; sk_patient_id '1' is a shared junk key and is excluded.
-- Some CSDS records have no sk_patient_id, so community contact counts are
-- a floor.

WITH complexity_dx AS (
    SELECT
        o.person_id,
        COUNT(DISTINCT o.mapped_concept_code) AS complexity_diagnosis_codes,
        MAX(o.clinical_effective_date) AS latest_complexity_diagnosis_date
    FROM {{ ref('stg_olids_observation') }} AS o
    INNER JOIN {{ ref('children_complexity_diagnosis_codes') }} AS c
        ON o.mapped_concept_code = c.snomed_code
    WHERE
        o.clinical_effective_date IS NOT NULL
        AND o.clinical_effective_date <= CURRENT_DATE()
    GROUP BY o.person_id
),

op_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_sus_op_appointment') }}
    WHERE
        appointment_attended_or_dna IN ('5', '6')
        AND start_date <= CURRENT_DATE()
),

paediatric_op AS (
    SELECT
        op.sk_patient_id,
        COUNT(DISTINCT op.visit_occurrence_id) AS paediatric_op_appointments_12mo
    FROM {{ ref('int_sus_op_appointment') }} AS op
    INNER JOIN {{ ref('paediatric_treatment_function_codes') }} AS tfc
        ON op.treatment_function_code = tfc.treatment_function_code
    CROSS JOIN op_max_date AS m
    WHERE
        op.appointment_attended_or_dna IN ('5', '6')
        AND op.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
        AND op.sk_patient_id IS NOT NULL
        AND op.sk_patient_id != '1'
    GROUP BY op.sk_patient_id
),

mh_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_mhsds_spell_encounters') }}
    WHERE start_date <= CURRENT_DATE()
),

-- Spells counted by start date only: end_date/duration semantics are being
-- reworked (orphaned open spells), start dates are stable.
mh_spells AS (
    SELECT
        s.sk_patient_id,
        COUNT(DISTINCT s.encounter_id) AS mh_inpatient_stays_total,
        COUNT(DISTINCT CASE
            WHEN s.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
                THEN s.encounter_id
        END) AS mh_inpatient_stays_12mo
    FROM {{ ref('int_mhsds_spell_encounters') }} AS s
    CROSS JOIN mh_max_date AS m
    WHERE
        s.sk_patient_id IS NOT NULL
        AND s.sk_patient_id != '1'
        AND s.start_date <= CURRENT_DATE()
    GROUP BY s.sk_patient_id
),

cc_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_csds_encounters') }}
    WHERE start_date <= CURRENT_DATE()
),

-- int_csds_encounters is already attended contacts only (one row per contact)
community AS (
    SELECT
        e.sk_patient_id,
        COUNT(*) AS community_contacts_12mo
    FROM {{ ref('int_csds_encounters') }} AS e
    CROSS JOIN cc_max_date AS m
    WHERE
        e.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
        AND e.sk_patient_id IS NOT NULL
        AND e.sk_patient_id != '1'
    GROUP BY e.sk_patient_id
),

children AS (
    SELECT
        d.person_id,
        d.sk_patient_id,
        d.is_active,
        d.age,

        ZEROIFNULL(l.ltc_count) AS ltc_count,
        ZEROIFNULL(l.ltc_count) >= 2 AS has_2plus_ltcs,

        ZEROIFNULL(dx.complexity_diagnosis_codes) AS complexity_diagnosis_codes,
        dx.latest_complexity_diagnosis_date,
        dx.person_id IS NOT NULL AS has_complexity_diagnosis,

        ZEROIFNULL(op.paediatric_op_appointments_12mo)
            AS paediatric_op_appointments_12mo,
        ZEROIFNULL(op.paediatric_op_appointments_12mo) >= 5
            AS has_5plus_paediatric_op_appointments,

        ZEROIFNULL(spec.op_spec_12mo) AS outpatient_specialties_12mo,
        ZEROIFNULL(spec.op_spec_12mo) >= 2 AS has_2plus_outpatient_specialties,

        ZEROIFNULL(mh.mh_inpatient_stays_12mo) AS mh_inpatient_stays_12mo,
        ZEROIFNULL(mh.mh_inpatient_stays_total) AS mh_inpatient_stays_total,
        ZEROIFNULL(mh.mh_inpatient_stays_12mo) >= 1 AS has_mh_inpatient_stay,

        ZEROIFNULL(cc.community_contacts_12mo) AS community_contacts_12mo,
        ZEROIFNULL(cc.community_contacts_12mo) >= 7
            AS has_7plus_community_contacts

    FROM {{ ref('dim_person_demographics') }} AS d
    LEFT JOIN {{ ref('int_segmentation_ltc_count') }} AS l
        ON d.person_id = l.person_id
    LEFT JOIN complexity_dx AS dx
        ON d.person_id = dx.person_id
    LEFT JOIN paediatric_op AS op
        ON d.sk_patient_id = op.sk_patient_id
    LEFT JOIN {{ ref('fct_person_sus_op_recent') }} AS spec
        ON d.sk_patient_id = spec.sk_patient_id
    LEFT JOIN mh_spells AS mh
        ON d.sk_patient_id = mh.sk_patient_id
    LEFT JOIN community AS cc
        ON d.sk_patient_id = cc.sk_patient_id
    WHERE d.age < 18
)

SELECT
    *,
    (
        has_2plus_ltcs
        OR has_complexity_diagnosis
        OR has_5plus_paediatric_op_appointments
        OR has_2plus_outpatient_specialties
        OR has_mh_inpatient_stay
        OR has_7plus_community_contacts
    ) AS meets_any_criterion,
    (
        has_2plus_ltcs::INT
        + has_complexity_diagnosis::INT
        + has_5plus_paediatric_op_appointments::INT
        + has_2plus_outpatient_specialties::INT
        + has_mh_inpatient_stay::INT
        + has_7plus_community_contacts::INT
    ) AS complexity_criteria_count
FROM children
