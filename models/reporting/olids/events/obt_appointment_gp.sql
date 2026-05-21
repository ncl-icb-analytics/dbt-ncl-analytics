/*
GP encounters from OLIDS appointments

Disclaimer: needs expert dataset reviewer to confirm suitability for purpose

Clinical Purpose:
- Establishing demand for gp services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select 
    id as encounter_id
    , person_id
    , start_date
    , actual_duration
    , national_slot_category_name
    , service_setting
    , appointment_status_code as code
    , appointment_status_display as display
from {{ ref('stg_olids_appointment') }} oa
where CONTEXT_TYPE = 'Care Related Encounter' -- Coverage is good, needs review by dataset expert. Would recommend reviewing national slot type and type more to further refine, and consider pulling any relevant seeming slots from other contexts