{{ config(materialized = 'table') }}

/*
ECDS clinical codes per attendance, as ordered arrays.

One row per attendance that has at least one coded diagnosis, investigation,
treatment or comorbidity. Arrays hold every code in source sequence order;
nothing is truncated. For descriptions and one-row-per-code analysis use
int_sus_uec_diagnosis (diagnoses) and int_sus_uec_procedure (investigations,
treatments, comorbidities via observation_type).
*/

with diagnoses as (
    select
        primarykey_id
        , array_agg(code) within group (order by snomed_id) as secondary_diagnosis_codes
        , count(*) as n_secondary_diagnoses
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
    where code is not null
      and snomed_id >= 2  -- position 1 is the primary diagnosis, carried on the encounter
    group by primarykey_id
),

investigations as (
    select
        primarykey_id
        , array_agg(code) within group (order by snomed_id) as investigation_codes
        , count(*) as n_investigations
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
    where code is not null
    group by primarykey_id
),

treatments as (
    select
        primarykey_id
        , array_agg(code) within group (order by snomed_id) as treatment_codes
        , count(*) as n_treatments
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }}
    where code is not null
    group by primarykey_id
),

comorbidities as (
    select
        primarykey_id
        , array_agg(code) within group (order by comorbidities_id) as comorbidity_codes
        , count(*) as n_comorbidities
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }}
    where code is not null
    group by primarykey_id
),

attendances as (
    select primarykey_id from diagnoses
    union
    select primarykey_id from investigations
    union
    select primarykey_id from treatments
    union
    select primarykey_id from comorbidities
)

select
    a.primarykey_id as visit_occurrence_id
    , d.secondary_diagnosis_codes
    , coalesce(d.n_secondary_diagnoses, 0) as n_secondary_diagnoses
    , i.investigation_codes
    , coalesce(i.n_investigations, 0) as n_investigations
    , t.treatment_codes
    , coalesce(t.n_treatments, 0) as n_treatments
    , c.comorbidity_codes
    , coalesce(c.n_comorbidities, 0) as n_comorbidities
from attendances as a
left join diagnoses as d on a.primarykey_id = d.primarykey_id
left join investigations as i on a.primarykey_id = i.primarykey_id
left join treatments as t on a.primarykey_id = t.primarykey_id
left join comorbidities as c on a.primarykey_id = c.primarykey_id
