-- Asserts the qadmissions_eth2016_to_ethrisk9 seed contains every
-- distinct `ethnicity` subcategory value present in
-- dim_person_ethnicity_combi. If a new ethnicity value appears in the
-- population (e.g. a future ETH2016 cluster, or an event-source
-- subcategory not previously seen) it would silently be COALESCEd to
-- ethrisk = 1 in int_qadmissions_ethrisk - hiding data drift.
--
-- The test fails when the SELECT returns rows. It returns one row per
-- ethnicity value that is in the live data but not in the mapping seed.

with population as (
    select distinct ethnicity
    from {{ ref('dim_person_ethnicity_combi') }}
    where ethnicity is not null
),

mapping as (
    select distinct ethnicity
    from {{ ref('qadmissions_eth2016_to_ethrisk9') }}
)

select p.ethnicity
from population p
left join mapping m
    on p.ethnicity = m.ethnicity
where m.ethnicity is null
