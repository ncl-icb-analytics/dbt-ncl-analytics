-- Asserts the qadmissions_lab_thresholds seed contains the two rows the
-- int_qadmissions_features lab_thresholds CTE pivots on: (haemoglobin, low)
-- and (platelets, high). If either row is missing the CTE's MAX(CASE ...)
-- silently returns NULL, the hb_value < hb_threshold and
-- platelet_value > platelet_threshold comparisons become NULL, and the outer
-- COALESCE(..., FALSE) flips c_hb / high_platelet to FALSE for every person.
--
-- The test fails when the SELECT returns rows. It returns one row per
-- required (measurement, direction) combination that is absent from the seed.

with required(measurement, direction) as (
    select 'haemoglobin', 'low'  union all
    select 'platelets',   'high'
),
present as (
    select distinct measurement, direction
    from {{ ref('qadmissions_lab_thresholds') }}
    where threshold is not null
)
select r.measurement, r.direction
from required r
left join present p
  on p.measurement = r.measurement
 and p.direction   = r.direction
where p.measurement is null
