-- int_sus_uec_encounter_clinical_codes must hold every coded child record:
-- array sizes equal the number of non-null codes in each staging feed.
with expected as (
    select primarykey_id, count(*) as n
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
    where code is not null
    group by primarykey_id
)

select
    c.visit_occurrence_id
    , c.n_investigations
    , e.n as staging_investigations
from {{ ref('int_sus_uec_encounter_clinical_codes') }} as c
full outer join expected as e
    on c.visit_occurrence_id = e.primarykey_id
where coalesce(c.n_investigations, 0) <> coalesce(e.n, 0)
   or coalesce(array_size(c.investigation_codes), 0) <> coalesce(e.n, 0)
