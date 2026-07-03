/*
OLIDS prescribing cost per person over the rolling 12-month SLAM window
(NCL only - OLIDS is EMIS NCL).

Uses MEDICATION_ORDER.estimated_cost - EMIS's per-order cost estimate.
Checked May 2025 - Apr 2026: 100% populated, 95.7% positive, window total
GBP 239m vs GBP 238m when the same orders are priced at EPD mean actual
cost per item by BNF code (0.4% apart), and the high tail is negligible
(orders > GBP 5k carry GBP 1.3m). Basis is an estimate, not reimbursed
spend - order-to-dispensing slippage and tariff drift are uncorrected.

orders_costed counts orders with a positive estimate.
*/

{{ config(materialized = 'table') }}

with slam_bounds as (
    select
        max(activity_month)                       as window_end,
        dateadd(month, -11, max(activity_month))  as window_start
    from {{ ref('fct_person_cost_by_month') }}
    where cost_source = 'SLAM'
)

select
    o.person_id,
    count(*)                                as prescription_orders_12m,
    count_if(o.estimated_cost > 0)          as orders_costed,
    round(sum(coalesce(o.estimated_cost, 0)), 2) as prescribing_cost_12m_modelled
from {{ ref('stg_olids_medication_order') }} as o
cross join slam_bounds as b
where o.clinical_effective_date >= b.window_start
  and o.clinical_effective_date < dateadd(month, 1, b.window_end)
  and o.person_id is not null
group by 1
