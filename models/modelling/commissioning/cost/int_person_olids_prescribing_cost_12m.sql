/*
OLIDS-modelled prescribing cost per person over the rolling 12-month SLAM
window (NCL only - OLIDS is EMIS NCL).

The EPD feed carries actual reimbursed cost but lags the window; OLIDS
medication orders are current but uncosted. This model prices each OLIDS
order in the window with the mean EPD actual cost per item for its BNF
code, over the latest 12 months EPD has (tiered match: full code ->
paragraph -> chapter). One order is treated as one dispensed item.

cost_basis is modelled: order-to-dispensing slippage, quantity variation
and price drift are all absorbed by the BNF-mean rate. orders_costed /
prescription_orders_12m says how much of the person's volume found a rate.
*/

{{ config(materialized = 'table') }}

with slam_bounds as (
    select
        max(activity_month)                       as window_end,
        dateadd(month, -11, max(activity_month))  as window_start
    from {{ ref('fct_person_cost_by_month') }}
    where cost_source = 'SLAM'
),

epd_bounds as (
    select max(date_trunc('month', processing_period_date)) as epd_end
    from {{ ref('stg_epd_pc_meds') }}
    where is_latest_submission
),

-- Mean actual cost per item by BNF, over EPD's latest 12 months
epd_items as (
    select
        upper(e.paid_bnf_code)     as bnf_code,
        sum(e.item_actual_cost) / 100 as cost,
        sum(e.item_count)          as items
    from {{ ref('stg_epd_pc_meds') }} as e
    cross join epd_bounds as b
    where e.is_latest_submission
      and date_trunc('month', e.processing_period_date)
          > dateadd(month, -12, b.epd_end)
    group by 1
),

rate_code as (
    select bnf_code, div0(cost, items) as rate
    from epd_items
    where items > 0
),

rate_paragraph as (
    select left(bnf_code, 6) as bnf_paragraph, div0(sum(cost), sum(items)) as rate
    from epd_items
    group by 1
    having sum(items) > 0
),

rate_chapter as (
    select left(bnf_code, 2) as bnf_chapter, div0(sum(cost), sum(items)) as rate
    from epd_items
    group by 1
    having sum(items) > 0
),

orders_in_window as (
    select
        o.person_id,
        upper(o.bnf_code) as bnf_code
    from {{ ref('int_medication_order_bnf') }} as o
    cross join slam_bounds as b
    where o.order_date >= b.window_start
      and o.order_date < dateadd(month, 1, b.window_end)
      and o.bnf_code is not null
)

select
    o.person_id,
    count(*)                                          as prescription_orders_12m,
    count_if(coalesce(rc.rate, rp.rate, rch.rate) is not null) as orders_costed,
    round(sum(coalesce(rc.rate, rp.rate, rch.rate, 0)), 2)     as prescribing_cost_12m_modelled
from orders_in_window as o
left join rate_code as rc on o.bnf_code = rc.bnf_code
left join rate_paragraph as rp on left(o.bnf_code, 6) = rp.bnf_paragraph
left join rate_chapter as rch on left(o.bnf_code, 2) = rch.bnf_chapter
group by 1
