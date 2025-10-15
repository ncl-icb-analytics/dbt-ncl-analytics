/*
GP encounters from OLIDS appointments

Disclaimer: needs expert dataset reviewer to confirm suitability for purpose

Clinical Purpose:
- Establishing demand for gp services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/
with gp_appt_concepts as (
    select distinct APPOINTMENT_STATUS_CONCEPT_ID
     from {{ref('stg_olids_appointment')}}
),
concept_mapping as (
    select cm.source_code_id as appt_stat_concept_code
        , c.code as code
        , c.display as display
    from  {{ref('stg_olids_concept_map')}} cm
    inner join {{ref('stg_olids_concept')}} c on cm.target_code_id = c.id
    where cm.source_code_id in (select APPOINTMENT_STATUS_CONCEPT_ID from gp_appt_concepts)
)


select person_id
    , start_date
    , actual_duration
    , national_slot_category_name
    , service_setting
    , code
    , display
from {{ ref('stg_olids_appointment') }} oa
left join concept_mapping cm on oa.APPOINTMENT_STATUS_CONCEPT_ID = cm.appt_stat_concept_code
where CONTEXT_TYPE = 'Care Related Encounter' -- Coverage is good, needs review by dataset expert. Would recommend reviewing national slot type and type more to further refine, and consider pulling any relevant seeming slots from other contexts