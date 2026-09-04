{{
    config(
        materialized='table',
        tags=['monthly', 'sus', 'ae']
    )
}}

select
    encounter.sk_encounter_id,
    max_by(organisation.organisation_code, care_home.period_start) as care_home_code
from {{ ref('int_sus_ae_monthly') }} as encounter
inner join {{ ref('stg_fact_patient_factcarehome') }} as care_home
    on encounter.sk_patient_id = care_home.sk_patient_id
   and encounter.arrival_date between cast(care_home.period_start as date)
                                      and coalesce(cast(care_home.period_end as date), '2050-12-31'::date)
left join {{ ref('stg_dictionary_dbo_organisation') }} as organisation
    on care_home.sk_organisation_id = organisation.sk_organisation_id
group by encounter.sk_encounter_id
