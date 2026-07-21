{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Medicines adherence — rolling PDC (Proportion of Days Covered). One row per
(person_id, drug_name, window_start): person-anchored monthly rolling
12-month windows from the first order of each (person, drug) refill chain.

Port of MedicationTableSnowpark.compute_pdc_rolling(window_length=12,
pdc_type=2, exclusive=True):
- pdc_type=2 "dynamic window": the PDC denominator is the exposure span
  (first order date to last order end within the window), not the fixed
  365-day window.
- exclusive=True: the window join uses a strict upper bound
  (order_date < window_end), and supply overlapping an early refill is
  subtracted (adjusted_overlap).
- Windows with no qualifying orders are retained with NULL exposure /
  covered_days / pdc, matching the original's left joins from the anchor set.
- An overall PDC per (person, drug) over the full chain span rides along on
  every window row, as in the original output.

Faithful vs corrected columns: the original 0-fills days_to_next_order on the
final order of each chain, so adjusted_overlap subtracts that order's entire
duration — the last prescription contributes ~0 covered days. pdc /
covered_days / overall_pdc replicate this exactly; the *_corrected columns
treat the final order's days_to_next_order as NULL so its supply counts.
The adjusted-overlap CTE is the only point of divergence between the two;
everything downstream is parallel sums and ratios.

Other divergences from the original:
- All five drug classes in one table (the original wrote one table per class;
  its per-class loop only existed to name tables — every grouping key already
  includes drug_class).
- The original's collect() round-trips and global order prefilter are dropped:
  they existed only to materialise literals for Snowpark and are semantic
  no-ops in a set-based join.
- months_between(...)::int is kept verbatim (fractional, rounds half away
  from zero) — do not replace with datediff('month', ...), which counts
  boundary crossings and would change the anchor count.
- A window_days > 0 guard is added to the pdc division: unreachable on the
  faithful path (covered_days > 0 implies a positive span) but belt-and-braces
  against Snowflake division-by-zero, which Snowpark's lazy CASE never hit.
*/

{% set window_length_months = 12 %}

with orders as (
    select
        person_id,
        drug_name,
        drug_class,
        order_date,
        calculated_duration,
        coalesce(order_enddate, order_date) as order_enddate_filled,
        -- Faithful: reproduces the last-order quirk (days_to_next_order = 0)
        case
            when days_to_next_order is not null
                and days_to_next_order < calculated_duration
                then calculated_duration - days_to_next_order
            else 0
        end as adjusted_overlap,
        -- Corrected: NULL on the last order -> no subtraction
        case
            when days_to_next_order_corrected is not null
                and days_to_next_order_corrected < calculated_duration
                then calculated_duration - days_to_next_order_corrected
            else 0
        end as adjusted_overlap_corrected
    from {{ ref('int_medication_adherence_order_durations') }}
),

overall_bounds as (
    select
        person_id,
        drug_name,
        drug_class,
        min(order_date) as overall_start,
        max(order_enddate_filled) as overall_end
    from orders
    group by person_id, drug_name, drug_class
),

-- One row per monthly anchor from overall_start to overall_end (inclusive
-- offset range, hence + 1 on the exclusive array_generate_range stop)
anchors as (
    select
        b.person_id,
        b.drug_name,
        b.drug_class,
        b.overall_start,
        b.overall_end,
        dateadd(month, f.value::int, b.overall_start) as window_start,
        dateadd(month, f.value::int + {{ window_length_months }}, b.overall_start) as window_end
    from overall_bounds b,
        lateral flatten(
            input => array_generate_range(
                0, months_between(b.overall_end, b.overall_start)::int + 1
            )
        ) f
),

windowed_orders as (
    select
        a.person_id,
        a.drug_name,
        a.drug_class,
        a.window_start,
        a.window_end,
        o.order_date,
        o.order_enddate_filled,
        o.adjusted_overlap,
        o.adjusted_overlap_corrected
    from anchors a
    inner join orders o
        on a.person_id = o.person_id
        and a.drug_name = o.drug_name
        and a.drug_class = o.drug_class
        and o.order_date < a.window_end
        and o.order_enddate_filled >= a.window_start
),

-- pdc_type=2 dynamic exposure span per window
window_exposure as (
    select
        person_id,
        drug_name,
        drug_class,
        window_start,
        window_end,
        min(order_date) as exposure_start,
        max(order_enddate_filled) as exposure_end
    from windowed_orders
    group by person_id, drug_name, drug_class, window_start, window_end
),

window_covered as (
    select
        wo.person_id,
        wo.drug_name,
        wo.drug_class,
        wo.window_start,
        wo.window_end,
        sum(
            case
                when least(wo.order_enddate_filled, e.exposure_end)
                    > greatest(wo.order_date, e.exposure_start)
                    then datediff(
                        day,
                        greatest(wo.order_date, e.exposure_start),
                        least(wo.order_enddate_filled, e.exposure_end)
                    )
                else 0
            end - wo.adjusted_overlap
        ) as covered_days,
        sum(
            case
                when least(wo.order_enddate_filled, e.exposure_end)
                    > greatest(wo.order_date, e.exposure_start)
                    then datediff(
                        day,
                        greatest(wo.order_date, e.exposure_start),
                        least(wo.order_enddate_filled, e.exposure_end)
                    )
                else 0
            end - wo.adjusted_overlap_corrected
        ) as covered_days_corrected
    from windowed_orders wo
    inner join window_exposure e
        on wo.person_id = e.person_id
        and wo.drug_name = e.drug_name
        and wo.drug_class = e.drug_class
        and wo.window_start = e.window_start
        and wo.window_end = e.window_end
    group by wo.person_id, wo.drug_name, wo.drug_class, wo.window_start, wo.window_end
),

-- Overall PDC over the full chain span [overall_start, overall_end]; the
-- clipping is kept for line-by-line parity with the original even though
-- every order lies within the bounds by construction
overall_covered as (
    select
        o.person_id,
        o.drug_name,
        o.drug_class,
        b.overall_start,
        b.overall_end,
        sum(
            case
                when least(o.order_enddate_filled, b.overall_end)
                    > greatest(o.order_date, b.overall_start)
                    then datediff(
                        day,
                        greatest(o.order_date, b.overall_start),
                        least(o.order_enddate_filled, b.overall_end)
                    )
                else 0
            end - o.adjusted_overlap
        ) as covered_days,
        sum(
            case
                when least(o.order_enddate_filled, b.overall_end)
                    > greatest(o.order_date, b.overall_start)
                    then datediff(
                        day,
                        greatest(o.order_date, b.overall_start),
                        least(o.order_enddate_filled, b.overall_end)
                    )
                else 0
            end - o.adjusted_overlap_corrected
        ) as covered_days_corrected
    from orders o
    inner join overall_bounds b
        on o.person_id = b.person_id
        and o.drug_name = b.drug_name
        and o.drug_class = b.drug_class
    group by o.person_id, o.drug_name, o.drug_class, b.overall_start, b.overall_end
),

overall_pdc as (
    select
        person_id,
        drug_name,
        drug_class,
        datediff(day, overall_start, overall_end) as total_exposure_days,
        -- No covered_days > 0 guard here — faithful to the original, which
        -- only guarded on total_exposure_days (overall_pdc can be <= 0)
        case
            when datediff(day, overall_start, overall_end) > 0
                then covered_days / datediff(day, overall_start, overall_end)
        end as overall_pdc,
        case
            when datediff(day, overall_start, overall_end) > 0
                then covered_days_corrected / datediff(day, overall_start, overall_end)
        end as overall_pdc_corrected
    from overall_covered
),

windows_assembled as (
    select
        a.person_id,
        a.drug_name,
        a.drug_class,
        a.window_start,
        a.window_end,
        a.overall_start,
        a.overall_end,
        e.exposure_start,
        e.exposure_end,
        c.covered_days,
        c.covered_days_corrected,
        datediff(day, e.exposure_start, e.exposure_end) as window_days
    from anchors a
    left join window_exposure e
        on a.person_id = e.person_id
        and a.drug_name = e.drug_name
        and a.drug_class = e.drug_class
        and a.window_start = e.window_start
        and a.window_end = e.window_end
    left join window_covered c
        on a.person_id = c.person_id
        and a.drug_name = c.drug_name
        and a.drug_class = c.drug_class
        and a.window_start = c.window_start
        and a.window_end = c.window_end
)

select
    w.person_id,
    w.drug_name,
    w.drug_class,
    w.window_start,
    w.window_end,
    w.exposure_start,
    w.exposure_end,
    w.covered_days,
    w.covered_days_corrected,
    w.window_days,
    case
        when w.covered_days > 0 and w.window_days > 0
            then w.covered_days / w.window_days
    end as pdc,
    case
        when w.covered_days_corrected > 0 and w.window_days > 0
            then w.covered_days_corrected / w.window_days
    end as pdc_corrected,
    w.overall_start,
    w.overall_end,
    op.total_exposure_days,
    op.overall_pdc,
    op.overall_pdc_corrected,
    2 as pdc_type,
    true as exclusive,
    {{ window_length_months }} as window_length_months
from windows_assembled w
left join overall_pdc op
    on w.person_id = op.person_id
    and w.drug_name = op.drug_name
    and w.drug_class = op.drug_class
