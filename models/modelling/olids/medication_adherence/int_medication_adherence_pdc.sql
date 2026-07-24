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

Faithful vs corrected columns — THEY MEASURE DIFFERENT THINGS.

The faithful columns (pdc, covered_days, window_days, overall_pdc,
total_exposure_days) replicate the original AIC measure exactly, including
three artefacts:
1. Last-order quirk: the original 0-fills days_to_next_order on the final
   order of each chain, so adjusted_overlap subtracts that order's entire
   duration — the last prescription contributes ~0 covered days.
2. No right-censoring: supply intervals project past the build date and
   count as covered — unobservable days enter numerator and denominator.
3. No frame clipping: the window is only a selection frame; exposure and
   supply overhang beyond [window_start, window_end] still count, so
   covered-only overhang days at either end pull window PDC toward 1.

The *_corrected window columns are a WINDOWED INTERVAL PDC over observed
days: everything — supply intervals, the exposure denominator and the
early-refill stockpile subtraction — is clipped to the window frame
[window_start, least(window_end, current_date)], and the final order's
supply counts (days_to_next_order_corrected is NULL on last orders).
pdc_corrected therefore answers "of the observed days inside this
12-month frame while the person was exposed, how many had supply" —
whereas faithful pdc follows the AIC exposure-span semantics.

The overall_*_corrected columns have no frame (their bounds ARE the chain),
so they are quirk-fixed and right-censored at current_date only.

Consequences:
- The early-refill subtraction for corrected is derived from the stockpile
  region [order_date + days_to_next, supply end] clipped per measurement
  context (overlap_region_start below); date-based arithmetic, vs the
  faithful subtraction's fractional duration difference.
- Corrected is NOT guaranteed >= faithful: frame clipping removes
  covered-only overhang days that faithful counts, and even for windows
  whose exposure lies fully inside the observed frame the two subtractions
  disagree by sub-day rounding (fractional duration difference vs
  date-based datediff over the same region — up to a day per early-refill
  order). The yml comparison test is therefore tolerance-based and
  warn-severity, restricted to fully observed in-frame windows.
- Windows whose frame lies entirely in the future yield NULL corrected
  measures (nothing observed inside the frame).
- For chains fully in the past, overall_pdc vs overall_pdc_corrected
  isolates the last-order quirk alone (used by validation section 7).

Other divergences from the original:
- All five drug classes in one table (the original wrote one table per class;
  its per-class loop only existed to name tables — every grouping key already
  includes drug_class).
- The original's collect() round-trips and global order prefilter are dropped:
  they existed only to materialise literals for Snowpark and are semantic
  no-ops in a set-based join.
- months_between(...)::int is kept verbatim (fractional, rounds half away
  from zero) — do not replace with datediff('month', ...), which counts
  boundary crossings and would change the anchor count. Anchor generation
  uses the UNCENSORED overall_end (faithful window grid).
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
        -- Right-censored supply end for the corrected measures
        least(coalesce(order_enddate, order_date), current_date) as order_enddate_censored,
        -- Faithful: reproduces the last-order quirk (days_to_next_order = 0)
        case
            when days_to_next_order is not null
                and days_to_next_order < calculated_duration
                then calculated_duration - days_to_next_order
            else 0
        end as adjusted_overlap,
        -- Corrected: start of the early-refill stockpile region
        -- [order_date + days_to_next, supply end]. NULL when there is no
        -- early refill (including last orders, whose days_to_next_order
        -- _corrected is NULL). The region is clipped to each measurement
        -- context downstream (window frame / overall span / current_date)
        -- so days never counted as covered are never subtracted either.
        case
            when days_to_next_order_corrected is not null
                and days_to_next_order_corrected < calculated_duration
                then dateadd(day, days_to_next_order_corrected, order_date)
        end as overlap_region_start
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
        o.order_enddate_censored,
        o.adjusted_overlap,
        o.overlap_region_start
    from anchors a
    inner join orders o
        on a.person_id = o.person_id
        and a.drug_name = o.drug_name
        and a.drug_class = o.drug_class
        and o.order_date < a.window_end
        and o.order_enddate_filled >= a.window_start
),

-- pdc_type=2 dynamic exposure span per window (raw, for the faithful
-- measures; the corrected denominator clips these bounds to the frame in
-- windows_assembled)
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
        -- Faithful: uncensored intervals, uncensored exposure bounds
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
        -- Corrected: windowed interval PDC numerator — supply AND the
        -- early-refill stockpile subtraction clipped to the observed frame
        -- [window_start, least(window_end, current_date)]
        -- (order_enddate_censored is already capped at current_date)
        sum(
            case
                when least(wo.order_enddate_censored, wo.window_end)
                    > greatest(wo.order_date, wo.window_start)
                    then datediff(
                        day,
                        greatest(wo.order_date, wo.window_start),
                        least(wo.order_enddate_censored, wo.window_end)
                    )
                else 0
            end
            - case
                when wo.overlap_region_start is not null
                    then greatest(
                        0,
                        datediff(
                            day,
                            greatest(wo.overlap_region_start, wo.window_start),
                            least(wo.order_enddate_censored, wo.window_end)
                        )
                    )
                else 0
            end
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
        -- Corrected overall: no frame — right-censored at current_date only
        -- (via order_enddate_censored), with the stockpile subtraction
        -- clipped the same way
        sum(
            case
                when least(o.order_enddate_censored, b.overall_end)
                    > greatest(o.order_date, b.overall_start)
                    then datediff(
                        day,
                        greatest(o.order_date, b.overall_start),
                        least(o.order_enddate_censored, b.overall_end)
                    )
                else 0
            end
            - case
                when o.overlap_region_start is not null
                    then greatest(
                        0,
                        datediff(
                            day,
                            o.overlap_region_start,
                            least(o.order_enddate_censored, b.overall_end)
                        )
                    )
                else 0
            end
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
        datediff(day, overall_start, least(overall_end, current_date)) as total_exposure_days_corrected,
        -- No covered_days > 0 guard here — faithful to the original, which
        -- only guarded on total_exposure_days (overall_pdc can be <= 0)
        case
            when datediff(day, overall_start, overall_end) > 0
                then covered_days / datediff(day, overall_start, overall_end)
        end as overall_pdc,
        case
            when datediff(day, overall_start, least(overall_end, current_date)) > 0
                then covered_days_corrected / datediff(day, overall_start, least(overall_end, current_date))
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
        datediff(day, e.exposure_start, e.exposure_end) as window_days,
        -- Corrected denominator: exposure clipped to the observed frame
        -- (negative for frames entirely in the future -> NULL pdc via guard)
        datediff(
            day,
            greatest(e.exposure_start, a.window_start),
            least(e.exposure_end, a.window_end, current_date)
        ) as window_days_corrected
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
    w.window_days_corrected,
    case
        when w.covered_days > 0 and w.window_days > 0
            then w.covered_days / w.window_days
    end as pdc,
    case
        when w.covered_days_corrected > 0 and w.window_days_corrected > 0
            then w.covered_days_corrected / w.window_days_corrected
    end as pdc_corrected,
    w.overall_start,
    w.overall_end,
    op.total_exposure_days,
    op.total_exposure_days_corrected,
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
