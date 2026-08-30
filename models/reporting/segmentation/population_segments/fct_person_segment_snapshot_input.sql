{{ config(materialized='view') }}

-- Stable segmentation fields for prospective SCD2 change tracking.
-- Age and general demographics are excluded to avoid unrelated versions.

SELECT
    person_id,
    segment_number,
    segment_name,
    is_active,
    is_deceased,
    is_end_of_life,
    is_complex_adult,
    has_multiple_ltcs,
    has_single_ltc,
    is_complex_child,
    has_child_health_needs,
    ltc_count,
    ltc_list,
    adult_complexity_criteria_count,
    child_complexity_criteria_count,
    child_has_2plus_ltcs,
    child_has_complexity_diagnosis,
    child_has_5plus_paediatric_op_appointments,
    child_has_2plus_outpatient_specialties,
    child_has_mh_inpatient_stay,
    child_has_7plus_community_contacts
FROM {{ ref('fct_person_segment') }}
