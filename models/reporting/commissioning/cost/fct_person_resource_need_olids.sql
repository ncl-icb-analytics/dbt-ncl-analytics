-- NCL deep-dive: fct_person_resource_need (registered sub-ICB 93C) enriched
-- with OLIDS primary-care context - Cambridge comorbidity score, LTC register
-- count, frailty, GP appointment cost and the EPD prescribing breakout.
-- One row per patient, same grain as the base fact (the enrichment int is
-- sk-grain by construction, so the left join cannot fan out).
--
-- Cost columns:
--   actual_cost_12m already contains EPD prescribing; prescribing_cost_12m
--   is its breakout. GP appointment cost is additive -
--   total_cost_incl_gp_appointments_12m = actual + GP appointments.
--   olids_prescribing_cost_12m_modelled covers the whole window from OLIDS
--   orders x EPD BNF rates; total_cost_whole_person_12m swaps the partial
--   EPD component for it and adds GP appointments:
--   actual - EPD breakout + OLIDS-modelled rx + GP appointments.
{{ config(materialized = 'table') }}

select
    f.*,
    e.sk_patient_id is not null                   as has_olids_record,
    e.cambridge_comorbidity_score,
    coalesce(e.ltc_count, 0)                      as ltc_count,
    coalesce(e.qof_ltc_count, 0)                  as qof_ltc_count,
    e.frailty_severity,
    coalesce(e.gp_appointment_cost_12m, 0)        as gp_appointment_cost_12m,
    coalesce(e.gp_appointments_12m, 0)            as gp_appointments_12m,
    coalesce(e.prescribing_cost_12m, 0)           as prescribing_cost_12m,
    e.prescribing_months_covered,
    coalesce(e.olids_prescribing_cost_12m_modelled, 0) as olids_prescribing_cost_12m_modelled,
    coalesce(e.olids_prescription_orders_12m, 0)  as olids_prescription_orders_12m,
    f.actual_cost_12m
        + coalesce(e.gp_appointment_cost_12m, 0)  as total_cost_incl_gp_appointments_12m,
    f.actual_cost_12m
        - coalesce(e.prescribing_cost_12m, 0)
        + coalesce(e.olids_prescribing_cost_12m_modelled, 0)
        + coalesce(e.gp_appointment_cost_12m, 0)  as total_cost_whole_person_12m
from {{ ref('fct_person_resource_need') }} as f
left join {{ ref('int_resource_need_olids_enrichment') }} as e
    on f.sk_patient_id = e.sk_patient_id
where f.registered_sub_icb_code = '93C'
